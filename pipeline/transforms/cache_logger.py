# Cache size logger transform for Apache Beam pipeline
import apache_beam as beam
import logging
from apache_beam.transforms.userstate import ReadModifyWriteStateSpec
from apache_beam.coders import VarIntCoder

log = logging.getLogger(__name__)


class CacheSizeLogger(beam.DoFn):
    """
    Keeps a single global counter in per-key state and logs immediately.
    Input elements should all share the same key, e.g. ('__cache__', 1)
    """

    count_state = ReadModifyWriteStateSpec("cache_count", VarIntCoder())

    def process(self, kv, count_state=beam.DoFn.StateParam(count_state)):
        key, inc = kv  # key is constant '__cache__', inc is 1
        current = count_state.read() or 0
        current += int(inc)
        count_state.write(current)
        log.info("Content cache size: %d unique items", current)
        # No output needed - just return to satisfy Beam's iterator requirement
        return
