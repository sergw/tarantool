#include <iconv.h>
#include "iconv_wrap.h"

iconv_t
iconv_wrap_open(const char *tocode, const char *fromcode)
{
        return iconv_open(tocode, fromcode);
}

int
iconv_wrap_close(iconv_t cd)
{
        return iconv_close(cd);
}

size_t
iconv_wrap(iconv_t cd,
           char **inbuf, size_t *inbytesleft,
           char **outbuf, size_t *outbytesleft)
{
        return iconv(cd, inbuf, inbytesleft,
                     outbuf, outbytesleft);
}
