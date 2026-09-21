import json, io
d = json.load(io.open(r"docs\data\alerts.json", encoding="utf-8"))
seen = set()
for a in d["alerts"]:
    k = (a.get("group"), a.get("metric"))
    if k in seen: continue
    seen.add(k)
    print(f"\n[{a['severity']}] {k[0]}/{k[1]}: {a['message'][:66]}")
    print(f"   LIKELY: {a.get('cause','')[:100]}")
    print(f"   CHECK : {a.get('check','')[:100]}")
