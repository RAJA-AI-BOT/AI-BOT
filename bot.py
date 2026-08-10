ls = len(market_cache)

    return jsonify({
        "status": "success",
        "service": "RAJA AI backend",
        "yahoo_pairs": len(YAHOO_SYMBOLS),
        "unique_yahoo_symbols": len(UNIQUE_YAHOO_SYMBOLS),
        "cached_symbols": cached_symbols,
        "cache_duration_seconds": CACHE_DURATION,
    })


@app.route("/verify-license", methods=["POST"])
def verify_license():
    data = request.get_json(silent=True) or {}

    key = str(data.get("key", "")).strip()
    user = str(data.get("user", "")).strip()
    device = str(data.get("device", "")).strip()

    if not key or not user or not device:
        return jsonify({
            "status": "error",
            "message": "Key, user and device are required.",
        }), 400

    licenses = load_licenses()
    record = licenses.get(key)

    if not record or not record.get("active", False):
        return jsonify({
            "status": "error",
            "message": "Invalid or revoked license key.",
        }), 401

    bound_device = record.get("device")
    bound_user = record.get("user")

    if bound_device and bound_device != device:
        return jsonify({
            "status": "error",
            "message": "This key is already bound to another device.",
        }), 403

    if bound_user and bound_user != user:
        return jsonify({
            "status": "error",
            "message": "This key is already assigned to another user.",
        }), 403

    record["device"] = device
    record["user"] = user
    record["last_verified_at"] = int(time.time())
    licenses[key] = record
    save_licenses(licenses)

    return jsonify({
        "status": "success",
        "message": "License verified successfully.",
        "user": user,
        "device_bound": True,
    })


@app.route("/admin/generate-key", methods=["POST"])
def admin_generate_key():
    data = request.get_json(silent=True) or {}

    password = str(data.get("password", ""))
    user = str(data.get("user", "")).strip()

    if password != ADMIN_PASSWORD:
        return jsonify({
            "status": "error",
            "message": "Incorrect admin password.",
        }), 403

    if not user:
        return jsonify({
            "status": "error",
            "message": "User Telegram ID / UID is required.",
        }), 400

    licenses = load_licenses()

    while True:
        key = "RAJA-VIP-" + secrets.token_hex(4).upper() + "-2026"
        if key not in licenses:
            break

    licenses[key] = {
        "active": True,
        "user": user,
        "device": None,
        "created_at": int(time.time()),
    }

    save_licenses(licenses)

    return jsonify({
        "status": "success",
        "message": "License created.",
        "key": key,
        "user": user,
    })


@app.route("/admin/revoke-key", methods=["POST"])
def admin_revoke_key():
    data = request.get_json(silent=True) or {}

    password = str(data.get("password", ""))
    key = str(data.get("key", "")).strip()

    if password != ADMIN_PASSWORD:
        return jsonify({
            "status": "error",
            "message": "Incorrect admin password.",
        }), 403

    licenses = load_licenses()

    if key not in licenses:
        return jsonify({
            "status": "error",
            "message": "License key not found.",
        }), 404

    licenses[key]["active"] = False
    licenses[key]["revoked_at"] = int(time.time())
    save_licenses(licenses)

    return jsonify({
        "status": "success",
        "message": "License revoked.",
        "key": key,
    })


@app.route("/scan", methods=["POST"])
def scan_markets():
    data = request.get_json(silent=True) or {}
    selected_pair = str(data.get("pair", "")).strip()

    # The current frontend normally scans each pair itself.
    # This branch is retained for direct API use.
    if (
        not selected_pair
        or "Auto Scan Best Pair" in selected_pair
    ):
        best = None

        for pair in ALL_PAIRS:
            result = calculate_live_indicators(pair)
            if result["signal"] == "NO SIGNAL":
                continue

            if best is None or result["score"] > best["score"]:
                best = result

        if best is None:
            return jsonify({
                "status": "success",
                "data": {
                    "pair": None,
                    "score": 0,
                    "signal": "NO SIGNAL",
                    "reason": "No configured pair reached valid confluence.",
                },
            })

        return jsonify({
            "status": "success",
            "data": best,
        })

    if selected_pair not in YAHOO_SYMBOLS:
        return jsonify({
            "status": "error",
            "message": f"Unsupported pair: {selected_pair}",
            "data": no_signal_result(
                selected_pair,
                "Pair is not configured in Yahoo mapping.",
            ),
        }), 400

    result = calculate_live_indicators(selected_pair)

    return jsonify({
        "status": "success",
        "data": result,
    })


# Start background cache warmer only after all functions/routes exist.
poller_thread = threading.Thread(
    target=background_market_poller,
    daemon=True,
)
poller_thread.start()


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(
        host="0.0.0.0",
        port=port,
        debug=False,
        threaded=True,
    )
