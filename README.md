# DigiSpots

    python -m venv .venv && . .venv/bin/activate
    pip install -r requirements.txt
    uvicorn server:app --host 127.0.0.1 --port 8000

Env vars: PSKR_TOPIC (default pskr/filter/v2/+/FT8/#), TTL_MINUTES (60), MQTT_PORT (1884), DB_PATH.
Put nginx/Caddy in front for HTTPS; proxy /ws with WebSocket upgrade headers.
Check live payload field names at mqtt.pskreporter.info and credit PSK Reporter on the page.

## Embed in another site

    <iframe src="https://pskmap.example.com/?embed=1&call=GI8RPM&mins=60"
            style="width:100%;height:650px;border:0;border-radius:6px" loading="lazy"></iframe>

URL options: embed=1, call=, band= (e.g. 20m), mode= (e.g. FT8), mins= (5/15/30/60).
Do not add the .dark-embed invert filter; the map is already dark.
