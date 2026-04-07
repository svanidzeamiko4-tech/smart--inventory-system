import json
import random
import xml.etree.ElementTree as ET


def parse_rs_payload(payload_text):
    payload_text = (payload_text or "").strip()
    if not payload_text:
        return {"waybill_no": "", "store": "", "items": []}

    if payload_text.startswith("<"):
        root = ET.fromstring(payload_text)
        waybill_no = root.findtext("waybill_no", default="")
        store = root.findtext("store", default="")
        items = []
        for item in root.findall("./items/item"):
            items.append(
                {
                    "product": item.findtext("product", default=""),
                    "qty": int(float(item.findtext("qty", default="0"))),
                    "cost_price": float(item.findtext("cost_price", default="0")),
                    "selling_price": float(item.findtext("selling_price", default="0")),
                }
            )
        return {"waybill_no": waybill_no, "store": store, "items": items}

    parsed = json.loads(payload_text)
    items = []
    for item in parsed.get("items", []):
        items.append(
            {
                "product": str(item.get("product", "")),
                "qty": int(float(item.get("qty", 0))),
                "cost_price": float(item.get("cost_price", 0)),
                "selling_price": float(item.get("selling_price", 0)),
            }
        )
    return {
        "waybill_no": str(parsed.get("waybill_no", "")),
        "store": str(parsed.get("store", "")),
        "items": items,
    }


def simulate_fetch_waybill(waybill_no):
    seed = sum(ord(ch) for ch in str(waybill_no))
    random.seed(seed)
    store = random.choice(["გლდანის ფილიალი", "ვაკის ფილიალი", "საბურთალოს ფილიალი"])
    item_count = random.randint(2, 4)
    known_products = ["Apple", "Banana", "Milk", "Bread", "Rice", "Pasta"]
    items = []
    for i in range(item_count):
        product_name = random.choice(known_products) if i < item_count - 1 else f"RS_Item_{seed % 1000}"
        qty = random.randint(3, 30)
        cost = round(random.uniform(0.5, 5.0), 2)
        sell = round(cost * random.uniform(1.2, 1.8), 2)
        items.append(
            {"product": product_name, "qty": qty, "cost_price": cost, "selling_price": sell}
        )

    payload = {"waybill_no": str(waybill_no), "store": store, "items": items}
    if seed % 2 == 0:
        return json.dumps(payload, ensure_ascii=False), "json"

    root = ET.Element("waybill")
    ET.SubElement(root, "waybill_no").text = str(waybill_no)
    ET.SubElement(root, "store").text = store
    items_el = ET.SubElement(root, "items")
    for item in items:
        item_el = ET.SubElement(items_el, "item")
        ET.SubElement(item_el, "product").text = item["product"]
        ET.SubElement(item_el, "qty").text = str(item["qty"])
        ET.SubElement(item_el, "cost_price").text = str(item["cost_price"])
        ET.SubElement(item_el, "selling_price").text = str(item["selling_price"])
    return ET.tostring(root, encoding="unicode"), "xml"
