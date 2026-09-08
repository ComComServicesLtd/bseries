#ifndef WEB_ASSETS_H
#define WEB_ASSETS_H

#include <string>
#include <stddef.h>

/// The admin page, compiled into the binary.
///
/// The image is scratch and holds one file, so there is nowhere to serve a
/// directory from; and serving the page from the database itself is what makes
/// it same origin, which is what lets it call the API with no cors_origin set
/// and no second server in the picture.
///
/// web_assets.cpp is generated from web/ by tools/embed.cpp. Run `make web`
/// after editing anything under web/.

typedef struct {
    const char *url;
    const unsigned char *data;
    size_t length;
    const char *content_type;
} WEB_ASSET;

/// The asset registered for a URL, or NULL.
const WEB_ASSET *webAsset(const std::string &url);

#endif // WEB_ASSETS_H
