EXCHANGES = {
    "parser.jobs": {"type": "direct", "durable": True},
    "normalize.jobs": {"type": "direct", "durable": True},
    "delivery.events": {"type": "topic", "durable": True},
    "parser.dlx": {"type": "fanout", "durable": True},
    "normalize.dlx": {"type": "fanout", "durable": True},
    "delivery.dlx": {"type": "fanout", "durable": True},
}

QUEUES = {
    "parser.high": {"exchange": "parser.jobs", "routing_key": "high"},
    "parser.bulk": {"exchange": "parser.jobs", "routing_key": "bulk"},
    "normalize.high": {"exchange": "normalize.jobs", "routing_key": "high"},
    "normalize.bulk": {"exchange": "normalize.jobs", "routing_key": "bulk"},
    "card.updated": {"exchange": "delivery.events", "routing_key": "card.updated"},
    "review.required": {"exchange": "delivery.events", "routing_key": "review.required"},
    "parser.dead": {"exchange": "parser.dlx", "routing_key": ""},
    "normalize.dead": {"exchange": "normalize.dlx", "routing_key": ""},
    "delivery.dead": {"exchange": "delivery.dlx", "routing_key": ""},
}
