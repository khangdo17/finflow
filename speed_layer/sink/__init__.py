"""
Sink modules for the FinFlow speed layer.
redis_sink: writes fraud alerts to Redis lists and sorted sets.
kafka_sink: publishes fraud alerts to the fraud-alerts Kafka topic with circuit breaker.
"""
