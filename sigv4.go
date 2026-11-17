package s3liteobjectstore

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	sigV4Algorithm  = "AWS4-HMAC-SHA256"
	sigV4Service    = "s3"
	sigV4Terminator = "aws4_request"
	unsignedPayload = "UNSIGNED-PAYLOAD"
	maxClockSkew    = 15 * time.Minute
	maxPresignSec   = 604800
)

type authResult struct {
	payloadHash string
}

func (s *Server) authenticateRequest(r *http.Request, now time.Time) (authResult, *Error) {
	q := r.URL.Query()
	if q.Get("X-Amz-Algorithm") != "" || q.Get("X-Amz-Signature") != "" || q.Get("X-Amz-Credential") != "" {
		return s.authenticatePresigned(r, q, now)
	}
	authHeader := r.Header.Get("Authorization")
	if authHeader == "" {
		return authResult{}, ErrAccessDenied
	}
	return s.authenticateHeader(r, authHeader, now)
}

func (s *Server) authenticateHeader(r *http.Request, authHeader string, now time.Time) (authResult, *Error) {
	if !strings.HasPrefix(authHeader, sigV4Algorithm+" ") {
		return authResult{}, ErrAuthorizationHeaderMalformed
	}
	rest := strings.TrimSpace(strings.TrimPrefix(authHeader, sigV4Algorithm))
	parts := strings.Split(rest, ",")
	var credential, signedHeadersStr, signature string
	for _, part := range parts {
		part = strings.TrimSpace(part)
		k, v, ok := strings.Cut(part, "=")
		if !ok {
			return authResult{}, ErrAuthorizationHeaderMalformed
		}
		switch strings.TrimSpace(k) {
		case "Credential":
			credential = strings.TrimSpace(v)
		case "SignedHeaders":
			signedHeadersStr = strings.TrimSpace(v)
		case "Signature":
			signature = strings.TrimSpace(v)
		}
	}
	if credential == "" || signedHeadersStr == "" || signature == "" {
		return authResult{}, ErrAuthorizationHeaderMalformed
	}

	accessKey, dateStamp, region, service, term, ok := parseCredential(credential)
	if !ok || service != sigV4Service || term != sigV4Terminator || region != s.region {
		return authResult{}, ErrAuthorizationHeaderMalformed
	}
	secret, exists := s.credentials[accessKey]
	if !exists {
		return authResult{}, ErrInvalidAccessKeyId
	}

	amzDate := r.Header.Get("X-Amz-Date")
	if amzDate == "" {
		amzDate = r.Header.Get("Date")
	}
	reqTime, ok := parseAmzTime(amzDate)
	if !ok || reqTime.Format("20060102") != dateStamp {
		return authResult{}, ErrAccessDenied
	}
	if absDuration(now.Sub(reqTime)) > maxClockSkew {
		return authResult{}, ErrRequestTimeTooSkewed
	}

	payloadHash := r.Header.Get("X-Amz-Content-Sha256")
	if payloadHash == "" {
		return authResult{}, ErrAccessDenied
	}
	if strings.HasPrefix(payloadHash, "STREAMING-") {
		return authResult{}, ErrNotImplemented
	}
	if payloadHash != unsignedPayload && !isValidSHA256Hex(payloadHash) {
		return authResult{}, ErrXAmzContentSHA256Mismatch
	}

	signedHeaders := strings.Split(strings.ToLower(signedHeadersStr), ";")
	canonHeaders, signedHeadersJoined, ok := buildCanonicalHeaders(r, signedHeaders)
	if !ok {
		return authResult{}, ErrAccessDenied
	}

	canonURI := canonicalURIPath(r.URL.Path)
	canonQuery := canonicalQueryString(r.URL.Query(), false)
	canonReq := strings.Join([]string{
		r.Method,
		canonURI,
		canonQuery,
		canonHeaders,
		signedHeadersJoined,
		payloadHash,
	}, "\n")

	scope := dateStamp + "/" + region + "/" + sigV4Service + "/" + sigV4Terminator
	stringToSign := strings.Join([]string{
		sigV4Algorithm,
		r.Header.Get("X-Amz-Date"),
		scope,
		sha256Hex([]byte(canonReq)),
	}, "\n")
	if r.Header.Get("X-Amz-Date") == "" {
		stringToSign = strings.Join([]string{
			sigV4Algorithm,
			reqTime.UTC().Format("20060102T150405Z"),
			scope,
			sha256Hex([]byte(canonReq)),
		}, "\n")
	}

	expectedSig := computeSigV4(secret, dateStamp, region, sigV4Service, stringToSign)
	if !hmac.Equal([]byte(expectedSig), []byte(strings.ToLower(signature))) {
		return authResult{}, ErrSignatureDoesNotMatch
	}
	return authResult{payloadHash: payloadHash}, nil
}

func (s *Server) authenticatePresigned(r *http.Request, q url.Values, now time.Time) (authResult, *Error) {
	if q.Get("X-Amz-Algorithm") != sigV4Algorithm {
		return authResult{}, ErrAuthorizationHeaderMalformed
	}
	credential := q.Get("X-Amz-Credential")
	amzDate := q.Get("X-Amz-Date")
	expiresStr := q.Get("X-Amz-Expires")
	signedHeadersStr := q.Get("X-Amz-SignedHeaders")
	signature := q.Get("X-Amz-Signature")
	if credential == "" || amzDate == "" || expiresStr == "" || signedHeadersStr == "" || signature == "" {
		return authResult{}, ErrAccessDenied
	}

	accessKey, dateStamp, region, service, term, ok := parseCredential(credential)
	if !ok || service != sigV4Service || term != sigV4Terminator || region != s.region {
		return authResult{}, ErrAuthorizationHeaderMalformed
	}
	secret, exists := s.credentials[accessKey]
	if !exists {
		return authResult{}, ErrInvalidAccessKeyId
	}

	expiresSec, err := strconv.Atoi(expiresStr)
	if err != nil || expiresSec < 1 || expiresSec > maxPresignSec {
		return authResult{}, ErrAccessDenied
	}
	reqTime, ok := parseAmzTime(amzDate)
	if !ok || reqTime.Format("20060102") != dateStamp {
		return authResult{}, ErrAccessDenied
	}
	if now.Before(reqTime.Add(-maxClockSkew)) {
		return authResult{}, ErrRequestTimeTooSkewed
	}
	if now.After(reqTime.Add(time.Duration(expiresSec) * time.Second)) {
		return authResult{}, newError(ErrAccessDenied, "Request has expired")
	}

	signedHeaders := strings.Split(strings.ToLower(signedHeadersStr), ";")
	canonHeaders, signedHeadersJoined, ok := buildCanonicalHeaders(r, signedHeaders)
	if !ok {
		return authResult{}, ErrAccessDenied
	}

	payloadHash := r.Header.Get("X-Amz-Content-Sha256")
	if payloadHash == "" {
		payloadHash = unsignedPayload
	}
	if strings.HasPrefix(payloadHash, "STREAMING-") {
		return authResult{}, ErrNotImplemented
	}

	canonURI := canonicalURIPath(r.URL.Path)
	canonQuery := canonicalQueryString(q, true)
	canonReq := strings.Join([]string{
		r.Method,
		canonURI,
		canonQuery,
		canonHeaders,
		signedHeadersJoined,
		payloadHash,
	}, "\n")

	scope := dateStamp + "/" + region + "/" + sigV4Service + "/" + sigV4Terminator
	stringToSign := strings.Join([]string{
		sigV4Algorithm,
		amzDate,
		scope,
		sha256Hex([]byte(canonReq)),
	}, "\n")

	expectedSig := computeSigV4(secret, dateStamp, region, sigV4Service, stringToSign)
	if !hmac.Equal([]byte(expectedSig), []byte(strings.ToLower(signature))) {
		return authResult{}, ErrSignatureDoesNotMatch
	}
	return authResult{payloadHash: payloadHash}, nil
}

func parseCredential(cred string) (accessKey, date, region, service, term string, ok bool) {
	parts := strings.Split(cred, "/")
	if len(parts) != 5 {
		return "", "", "", "", "", false
	}
	if parts[0] == "" || len(parts[1]) != 8 {
		return "", "", "", "", "", false
	}
	return parts[0], parts[1], parts[2], parts[3], parts[4], true
}

func parseAmzTime(val string) (time.Time, bool) {
	if t, err := time.Parse("20060102T150405Z", val); err == nil {
		return t, true
	}
	if t, err := http.ParseTime(val); err == nil {
		return t.UTC(), true
	}
	return time.Time{}, false
}

func absDuration(d time.Duration) time.Duration {
	if d < 0 {
		return -d
	}
	return d
}

func buildCanonicalHeaders(r *http.Request, signedHeaders []string) (string, string, bool) {
	if len(signedHeaders) == 0 {
		return "", "", false
	}
	sorted := make([]string, len(signedHeaders))
	copy(sorted, signedHeaders)
	sort.Strings(sorted)

	hostIncluded := false
	var b strings.Builder
	for _, h := range sorted {
		h = strings.TrimSpace(strings.ToLower(h))
		if h == "" {
			return "", "", false
		}
		if h == "host" {
			hostIncluded = true
			hostVal := r.Host
			if hostVal == "" {
				hostVal = r.Header.Get("Host")
			}
			b.WriteString("host:")
			b.WriteString(collapseSpaces(strings.TrimSpace(hostVal)))
			b.WriteByte('\n')
			continue
		}
		vals := r.Header.Values(h)
		if len(vals) == 0 {
			return "", "", false
		}
		cleaned := make([]string, len(vals))
		for i, v := range vals {
			cleaned[i] = collapseSpaces(strings.TrimSpace(v))
		}
		b.WriteString(h)
		b.WriteByte(':')
		b.WriteString(strings.Join(cleaned, ","))
		b.WriteByte('\n')
	}
	if !hostIncluded {
		return "", "", false
	}
	return b.String(), strings.Join(sorted, ";"), true
}

func collapseSpaces(s string) string {
	if !strings.Contains(s, "  ") {
		return s
	}
	return strings.Join(strings.Fields(s), " ")
}

func canonicalURIPath(p string) string {
	if p == "" {
		return "/"
	}
	segments := strings.Split(p, "/")
	for i, seg := range segments {
		segments[i] = rfc3986Encode(seg)
	}
	out := strings.Join(segments, "/")
	if !strings.HasPrefix(out, "/") {
		out = "/" + out
	}
	return out
}

func canonicalQueryString(q url.Values, excludeSignature bool) string {
	if len(q) == 0 {
		return ""
	}
	type pair struct {
		k string
		v string
	}
	var pairs []pair
	for k, vals := range q {
		if excludeSignature && k == "X-Amz-Signature" {
			continue
		}
		ek := rfc3986Encode(k)
		if len(vals) == 0 {
			pairs = append(pairs, pair{k: ek, v: ""})
			continue
		}
		for _, v := range vals {
			pairs = append(pairs, pair{k: ek, v: rfc3986Encode(v)})
		}
	}
	sort.Slice(pairs, func(i, j int) bool {
		if pairs[i].k != pairs[j].k {
			return pairs[i].k < pairs[j].k
		}
		return pairs[i].v < pairs[j].v
	})
	out := make([]string, len(pairs))
	for i, p := range pairs {
		out[i] = p.k + "=" + p.v
	}
	return strings.Join(out, "&")
}

func rfc3986Encode(s string) string {
	const hexUpper = "0123456789ABCDEF"
	var b strings.Builder
	b.Grow(len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '-' || c == '_' || c == '.' || c == '~' {
			b.WriteByte(c)
		} else {
			b.WriteByte('%')
			b.WriteByte(hexUpper[c>>4])
			b.WriteByte(hexUpper[c&0x0f])
		}
	}
	return b.String()
}

func hmacSHA256(key []byte, data string) []byte {
	h := hmac.New(sha256.New, key)
	h.Write([]byte(data))
	return h.Sum(nil)
}

func computeSigV4(secret, dateStamp, region, service, stringToSign string) string {
	kDate := hmacSHA256([]byte("AWS4"+secret), dateStamp)
	kRegion := hmacSHA256(kDate, region)
	kService := hmacSHA256(kRegion, service)
	kSigning := hmacSHA256(kService, sigV4Terminator)
	sig := hmacSHA256(kSigning, stringToSign)
	return hex.EncodeToString(sig)
}

func sha256Hex(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func isValidSHA256Hex(s string) bool {
	if len(s) != 64 {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')) {
			return false
		}
	}
	return true
}
