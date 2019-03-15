```
RUST_LOG='info' \
RELAY_BIND='127.0.0.1:3001' \
RELAY_CONFIG='{
	"root": {
		"type": "Static",
		"address": "private.moneyd",
		"asset_scale": 9,
		"asset_code": "XRP"
	},
	"auth_tokens": [
		"relay_secret_1",
		"relay_secret_2"
	],
	"routes": [
		{
			"target_prefix": "private.moneyd.3000.",
			"next_hop": {
				"type": "Bilateral",
				"endpoint": "http://127.0.0.1:3000",
				"auth": "secret_bilateral"
			}
		},
		{
			"target_prefix": "private.moneyd.",
			"next_hop": {
				"type": "Multilateral",
				"endpoint_prefix": "http://127.0.0.1:",
				"endpoint_suffix": "",
				"auth": "secret_multilateral"
			}
		}
	]
}' ilprelay
```
