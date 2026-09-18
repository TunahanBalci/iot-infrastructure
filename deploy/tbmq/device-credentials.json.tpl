{
  "_comment": [
    "TBMQ X.509 certificate-chain credentials, applied over REST by scripts/install/70-tbmq.sh.",
    "certCnPattern matches the CN of the issuing CA in the chain the client sends (leaf + CA).",
    "authRulesMapping: regex on the client certificate CN -> publish/subscribe topic regexes.",
    "${cn} is replaced by the (regex-quoted) client CN, so each tunnel may only publish its own topics.",
    "CN is a generated tunnel id (T000123, generator mode) or a profile id (TR-AVRASYA).",
    "Empty rule lists deny everything of that kind."
  ],
  "name": "iot-devices",
  "clientType": "DEVICE",
  "credentialsType": "X_509",
  "credentialsValue": {
    "certCnPattern": "${DOMAIN} device CA",
    "certCnIsRegex": false,
    "authRulesMapping": {
      "^(T[0-9]{6}|TR-[A-Z0-9-]{2,40})$": {
        "pubAuthRulePatterns": [
          "tunnels/${cn}/sensors/(start|middle|end)/(detections|health)",
          "simulator/[^/]+-${cn}/status"
        ],
        "subAuthRulePatterns": []
      },
      "^iot-viewer$": {
        "pubAuthRulePatterns": [],
        "subAuthRulePatterns": [
          "tunnels/.*",
          "simulator/.*"
        ]
      }
    }
  }
}
