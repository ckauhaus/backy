from backy.backends.chunked.chunk import Chunk
from backy.backends.chunked.file import File
from backy.backends.chunked.store import Store
import lzo
import os


SPACE_CHUNK = b' ' * Chunk.CHUNK_SIZE
SPACE_CHUNK_HASH = (
    'c01b5d75bfe6a1fa5bca6e492c5ab09a')


def test_chunk_read_write_update(tmpdir):
    store = Store(str(tmpdir))
    f = File(str(tmpdir / 'asdf'), store)

    chunk = Chunk(f, 1, store, None)
    chunk.write(0, b'asdf')
    chunk.write(4, b'bsdf')
    assert chunk.read(0) == (b'asdfbsdf', -1)
    chunk.write(1, b'xxxx')
    assert chunk.read(0) == (b'axxxxsdf', -1)


def test_chunk_write_partial_offset(tmpdir):
    store = Store(str(tmpdir))
    f = File(str(tmpdir / 'asdf'), store)

    chunk = Chunk(f, 1, store, None)
    # Write data that fits exactly into this chunk. Nothing remains
    # to be written.
    result = chunk.write(0, SPACE_CHUNK)
    assert result == (Chunk.CHUNK_SIZE, b'')
    # Write data that doesn't fit exactly into this chunk. This means
    # we have remaining data that needs to go into another chunk.
    result = chunk.write(10, SPACE_CHUNK)
    assert result == (Chunk.CHUNK_SIZE - 10, b' ' * 10)

    chunk.flush()
    assert chunk.hash == SPACE_CHUNK_HASH
    store_state = os.stat(store.chunk_path(SPACE_CHUNK_HASH))

    with open(store.chunk_path(chunk.hash), 'rb') as store_file:
        data = store_file.read()
        data = lzo.decompress(data)
        assert data == SPACE_CHUNK

    # Check that we can edit and flush again. Check that the store file
    # wasn't touched.
    chunk.write(0, b'      ')
    chunk.flush()
    assert store_state == os.stat(store.chunk_path(SPACE_CHUNK_HASH))


def test_chunk_read_existing_uncompressed(tmpdir):
    store = Store(str(tmpdir))
    with open(store.chunk_path('asdf', compressed=False), 'wb') as existing:
        existing.write(b'asdf')

    f = File(str(tmpdir / 'asdf'), store)

    chunk = Chunk(f, 1, store, 'asdf')
    assert chunk.read(0) == (b'asdf', -1)
    assert chunk.read(0, 10) == (b'asdf', 6)

    # Check that flushing a file that hasn't been written to does not fail.
    chunk.flush()


def test_chunk_read_existing_compressed(tmpdir):
    store = Store(str(tmpdir))
    with open(store.chunk_path('asdf'), 'wb') as existing:
        existing.write(lzo.compress(b'asdf'))

    f = File(str(tmpdir / 'asdf'), store)

    chunk = Chunk(f, 1, store, 'asdf')
    assert chunk.read(0) == (b'asdf', -1)
    assert chunk.read(0, 10) == (b'asdf', 6)

    # Check that flushing a file that hasn't been written to does not fail.
    chunk.flush()


def test_chunk_write_existing_uncompressed(tmpdir):
    store = Store(str(tmpdir))
    with open(store.chunk_path('asdf', compressed=False), 'wb') as existing:
        existing.write(b'asdf')

    f = File(str(tmpdir / 'asdf'), store)

    chunk = Chunk(f, 1, store, 'asdf')
    chunk.write(2, b'xxsdf')
    assert chunk.read(0) == (b'asxxsdf', -1)


def test_chunk_write_existing_compressed(tmpdir):
    store = Store(str(tmpdir))
    with open(store.chunk_path('asdf'), 'wb') as existing:
        existing.write(lzo.compress(b'asdf'))

    f = File(str(tmpdir / 'asdf'), store)

    chunk = Chunk(f, 1, store, 'asdf')
    chunk.write(2, b'xxsdf')
    assert chunk.read(0) == (b'asxxsdf', -1)