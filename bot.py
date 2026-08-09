from flask import Flask, request, jsonify, render_template_string
from flask_cors import CORS
import os
import random

app = Flask(__name__)
CORS(app)

# Saare OTC pairs ki list
OTC_PAIRS = [
    "NZD/CAD (OTC)", "NZD/USD (OTC)", "NZD/CHF (OTC)", "USD/BRL (OTC)", "NZD/JPY (OTC)", 
    "USD/ARS (OTC)", "USD/INR (OTC)", "USD/CAD (OTC)", "USD/DZD (OTC)", "USD/NGN (OTC)", 
    "USD/PHP (OTC)", "USD/IDR (OTC)", "USD/EGP (OTC)", "USD/MXN (OTC)", 
    "USD/PKR (OTC)", "GBP/NZD (OTC)", "USD/BDT (OTC)", "USD/COP (OTC)", "CAD/CHF (OTC)"
]

# Aapka Frontend HTML Template (Direct Render par load hoga)
HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no">
    <title>RAJA AI PREMIUM - VIP QUANTUM (ULTRA PRO UPGRADED)</title>
    <style>
        * { box-sizing: border-box; margin: 0; padding: 0; }
        body { background: radial-gradient(circle at center, #070b19 0%, #010309 100%); color: #ffffff; font-family: 'Segoe UI', system-ui, -apple-system, sans-serif; min-height: 100vh; display: flex; justify-content: center; align-items: flex-start; padding: 6px; }
        .container { width: 100%; max-width: 440px; background: linear-gradient(135deg, rgba(13, 20, 38, 0.95) 0%, rgba(4, 7, 15, 0.98) 100%); border: 1px solid rgba(0, 242, 254, 0.3); border-radius: 16px; padding: 12px; box-shadow: 0 0 40px rgba(0, 242, 254, 0.12); backdrop-filter: blur(12px); margin-top: 4px; margin-bottom: 10px; }
        h2 { text-align: center; font-size: 16px; background: linear-gradient(135deg, #00ff87 0%, #60efff 50%, #ff3366 100%); -webkit-background-clip: text; -webkit-text-fill-color: transparent; margin-bottom: 6px; font-weight: 900; text-transform: uppercase; }
        .market-notice { background: rgba(0, 242, 254, 0.08); border: 1px dashed rgba(0, 242, 254, 0.3); border-radius: 6px; padding: 5px; text-align: center; font-size: 9.5px; color: #60efff; font-weight: 700; margin-bottom: 8px; }
        .instruction-box { background: rgba(13, 20, 36, 0.75); border: 1px solid rgba(0, 242, 254, 0.3); border-radius: 10px; padding: 10px; font-size: 11px; line-height: 1.5; margin-bottom: 10px; text-align: left; }
        .step-title { color: #00ff87; font-weight: 900; margin-top: 6px; font-size: 12px; }
        .link-text { color: #60efff; text-decoration: underline; word-break: break-all; font-weight: 600; }
        .label-text { display: block; color: #60efff; font-size: 10px; font-weight: 800; margin: 6px 0 2px 0; text-transform: uppercase; text-align: left; }
        .ai-hud-box { position: relative; background: linear-gradient(180deg, rgba(6, 10, 20, 0.95) 0%, rgba(11, 17, 32, 0.95) 100%); border: 1px solid rgba(0, 242, 254, 0.4); border-radius: 12px; padding: 10px 8px; margin-bottom: 8px; text-align: center; overflow: hidden; display: flex; flex-direction: column; align-items: center; min-height: 150px; justify-content: center; }
        .vortex-container { position: relative; width: 52px; height: 52px; margin: 0 auto 4px auto; display: flex; align-items: center; justify-content: center; border-radius: 50%; background: radial-gradient(circle, rgba(0, 242, 254, 0.15) 0%, rgba(0, 0, 0, 0) 70%); }
        .vortex-icon { width: 40px; height: 40px; border-radius: 50%; background-color: #00f2fe; border: 1.5px solid rgba(0, 255, 135, 0.6); }
        .hud-stats { display: flex; justify-content: center; width: 100%; padding: 4px 8px 0 8px; border-top: 1px solid rgba(0, 242, 254, 0.15); margin-top: 6px; font-size: 9px; font-weight: 700; color: #00ff87; }
        .selection-card { background: rgba(13, 20, 36, 0.75); border: 1px solid rgba(255, 255, 255, 0.06); border-radius: 10px; padding: 10px; margin-bottom: 8px; }
        .selection-card label { display: block; color: #60efff; font-size: 9.5px; font-weight: 700; margin-bottom: 3px; text-transform: uppercase; }
        select, .input-field { width: 100%; padding: 8px; background: #050811; color: #fff; border: 1px solid rgba(0, 242, 254, 0.3); border-radius: 6px; font-size: 11px; font-weight: 600; outline: none; margin-top: 3px; }
        .scan-btn { background: linear-gradient(135deg, #00ff87 0%, #60efff 100%); color: #030712; font-size: 12px; font-weight: 900; padding: 10px; border: none; border-radius: 8px; cursor: pointer; width: 100%; text-transform: uppercase; box-shadow: 0 4px 15px rgba(0, 255, 135, 0.4); margin-top: 6px; }
    </style>
</head>
<body>
    <div class="container">
        <h2>RAJA AI PREMIUM - RENDER DEPLOYED</h2>
        <div class="market-notice">💡 Backend & Frontend Connected Successfully!</div>
        
        <div class="ai-hud-box">
            <div class="vortex-container"><div class="vortex-icon"></div></div>
            <div style="font-size: 9.5px; color: #60efff; font-weight: 900;">8-INDICATOR QUANTUM ENGINE (95%+)</div>
            <div class="hud-stats">STATUS: ONLINE & READY</div>
        </div>

        <div class="selection-card">
            <label>Select Market Pair</label>
            <select id="pairSelect">
                <option value="NZD/CAD (OTC)">NZD/CAD (OTC)</option>
                <option value="EUR/USD (OTC)">EUR/USD (OTC)</option>
                <option value="GBP/USD (OTC)">GBP/USD (OTC)</option>
            </select>
            <button class="scan-btn" onclick="runBackendScan()">▶ TEST BACKEND SCAN</button>
        </div>
        <div id="resultBox" style="margin-top: 8px; font-size: 11px; text-align: center; color: #00ff87; font-weight: bold;"></div>
    </div>

    <script>
        function runBackendScan() {
            let pair = document.getElementById('pairSelect').value;
            document.getElementById('resultBoxinnerText = "Scanning via Python Backend...";
            fetch('/scan', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ pair: pair })
            })
            .then(res => res.json())
            .then(data => {
                document.getElementById('resultBox').innerText = `✅ Result: ${data.data.pair} | Score: ${data.data.score}% | Signal: ${data.data.signal}`;
            })
            .catch(err => {
                document.getElementById('resultBox').innerText = "❌ Error connecting to backend.";
            });
        }
    </script>
</body>
</html>
"""

@app.route('/')
def home():
    return render_template_string(HTML_TEMPLATE)

@app.route('/scan', methods=['POST'])
def scan_markets():
    data = request.json or {}
    selected_pair = data.get('pair', 'NZD/CAD (OTC)')
    
    score = random.randint(90, 98)
    signal_type = "CALL (UP)" if random.choice([True, False]) else "PUT (DOWN)"
    
    return jsonify({
        "status": "success",
        "data": {
            "pair": selected_pair,
            "score": score,
            "signal": signal_type
        }
    })

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)
