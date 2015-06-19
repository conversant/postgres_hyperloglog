#ifndef _ENCODING_H_
#define _ENCODING_H_
/* Provides encoding and decoding to convert the estimator bytes into a human
 * readable form. Currently only base 64 encoding is provided. */

static unsigned b64_encode(const char *src, unsigned len, char *dst);
static unsigned b64_decode(const char *src, unsigned len, char *dst);
static unsigned b64_enc_len(const char *src, unsigned srclen);
static unsigned b64_dec_len(const char *src, unsigned srclen);

#endif // #ifndef _ENCODING_H_