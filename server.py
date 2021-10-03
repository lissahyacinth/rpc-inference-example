import pyarrow as pa
import pyarrow.flight as fl
import pyarrow.compute as comp

class TransformerFlightServer(fl.FlightServerBase):
    def __init__(self, host="localhost", location=None,
            tls_certificates=None, verify_client=False,
            root_certificates=None, auth_handler=None):
        super().__init__(
                location, auth_handler, tls_certificates, verify_client,
                root_certificates)
        self.flights = {}
        self.host = host
        self.tls_certificates = tls_certificates
        self.reply_schema = pa.schema(
                {
                    'a': 'int64'
                    }
                )

    def transform(self, data: pa.RecordBatch) -> pa.RecordBatch:
        length = len(data)
        repl_col = comp.multiply(data.column(0), pa.array([2] * length))
        return pa.RecordBatch.from_arrays(
                [repl_col],
                schema = self.reply_schema
                )


    def do_exchange(self, context, descriptor, reader, writer):
        writer.begin(self.reply_schema)
        # Chunk has type FlightStreamChunk
        for chunk in reader:
            # Chunk.data is a RecordBatch
            if chunk.app_metadata and chunk.data:
                repl = self.transform(chunk.data)
                writer.write_with_metadata(repl, chunk.app_metadata)
            elif chunk.app_metadata:
                writer.write_metadata(chunk.app_metadata)
            elif chunk.data:
                repl = self.transform(chunk.data)
                writer.write_batch(repl)

def test_main():
    data = pa.Table.from_arrays([
        pa.array(range(0, 12 * 25))
        ], names = ["a"])
    batches = data.to_batches(max_chunksize=12)

    with TransformerFlightServer() as server:
        client = fl.FlightClient(("localhost", server.port))
        descriptor = fl.FlightDescriptor.for_command(b"Transform")
        writer, reader = client.do_exchange(descriptor)
        with writer:
            writer.begin(data.schema)
            # TODO: Test as async thread for read/write
            for i, batch in enumerate(batches):
                buf = str(i).encode("utf-8")
                print(f"Input {batch.to_pydict()}")
                writer.write_with_metadata(batch, buf)
                chunk = reader.read_chunk()
                print(f"Reply {chunk.data.to_pydict()}")

if __name__ == '__main__':
    test_main()
