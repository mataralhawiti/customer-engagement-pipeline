# Redis writer transform for Apache Beam pipeline
import apache_beam as beam
import logging


class WriteToRedisDoFn(beam.DoFn):
    """DoFn for writing key-value pairs to Redis with TTL support"""

    def __init__(self, host: str, port: int, password: str = None, ttl: int = None):
        self._host = host
        self._port = port
        self._password = password
        self._ttl = ttl
        self._redis_client = None

        # Initialize metrics and logger
        self._ok = beam.metrics.Metrics.counter("redis", "writes_ok")
        self._err = beam.metrics.Metrics.counter("redis", "writes_error")
        self._log = logging.getLogger(self.__class__.__name__)

    def setup(self):
        """Initializes the Redis client once per worker/process."""
        import redis

        self._redis_client = redis.Redis(
            host=self._host,
            port=self._port,
            password=self._password,
            decode_responses=True,
        )

    def process(self, element):
        """Process a key-value pair and write to Redis"""
        k, v = element
        try:
            if self._ttl:
                self._redis_client.set(k, v, ex=self._ttl)
            else:
                self._redis_client.set(k, v)
            self._ok.inc()
        except Exception as e:
            self._err.inc()
            self._log.error("Redis write failed for key=%s: %s", k, e)

        # Yield nothing to satisfy Beam's iterator requirement
        return

    def teardown(self):
        """Closes the Redis connection (optional, as redis-py handles connection pooling)."""
        if self._redis_client:
            self._redis_client.connection_pool.disconnect()
