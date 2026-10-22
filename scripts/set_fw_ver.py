Import("env")

import datetime
import os
import re

# Date-based firmware version, as in JUUSOFLUX_HUB and JUUSOFLUX_DASH. CI passes
# the release tag via JF_FW_VER (e.g. "2026.10.22" or "v2026.10.22"); a local
# build without it takes today's UTC date. Compiled in as JF_FW_VER_STR and
# reported by @VER!.
ver = os.getenv("JF_FW_VER", "").strip()
if ver.startswith("v"):
    ver = ver[1:]

if not ver:
    ver = datetime.datetime.utcnow().strftime("%Y.%m.%d")

if not re.fullmatch(r"\d{4}\.\d{2}\.\d{2}", ver):
    raise ValueError(f"Invalid JF_FW_VER '{ver}'. Expected YYYY.MM.DD or vYYYY.MM.DD")

env.Append(BUILD_FLAGS=[f'-DJF_FW_VER_STR=\\"{ver}\\"'])
