import logging
import os
import re

from billiard import current_process

logger = logging.getLogger(__name__)


def configured_slots(raw=None):
    """cpu_id values this pool may use, taken from WORKER_CPU_SLOTS."""
    raw = os.environ.get('WORKER_CPU_SLOTS', '') if raw is None else raw
    return [int(slot) for slot in re.findall(r'-?\d+', str(raw))]


def resolve_pool_index(process=None):
    """Position of this process inside its pool, or None outside a pool.

    billiard assigns .index before forking, so a pool child inherits it. Name
    and identity are weaker signals kept as a safety net: both are global
    counters that keep growing as processes are recycled, so the caller wraps
    them into the available range.
    """
    process = current_process() if process is None else process

    index = getattr(process, 'index', None)
    if isinstance(index, int):
        return index

    match = re.search(r'-(\d+)$', str(getattr(process, 'name', '') or ''))
    if match:
        return int(match.group(1)) - 1

    identity = getattr(process, '_identity', None)
    if identity:
        try:
            return int(identity[-1]) - 1
        except (TypeError, ValueError):
            pass

    return None


def cpu_slot(slots=None, process=None):
    """Stable cpu_id for this process, kept inside the configured slot list."""
    slots = configured_slots() if slots is None else [int(slot) for slot in slots]
    index = resolve_pool_index(process)

    if not slots:
        return index or 0

    if index is None:
        slot = slots[os.getpid() % len(slots)]
        logger.warning('pool index unavailable, cpu_id %s derived from pid', slot)
        return slot

    if index >= len(slots):
        logger.warning(
            'pool index %s exceeds the %s configured slots, wrapping around',
            index,
            len(slots),
        )

    return slots[index % len(slots)]
