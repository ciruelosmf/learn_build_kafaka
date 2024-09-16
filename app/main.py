import socket
import struct

SUPPORTED_VERSIONS = [0, 1, 2, 3, 4]
UNSUPPORTED_VERSION_ERROR = 35

def main():
    print("Starting Kafka-like server...")

    server = socket.create_server(("localhost", 9092), reuse_port=True)
    
    while True:
        print("Waiting for a connection...")
        client_socket, address = server.accept()
        print(f"Connected by {address}")

        try:
            # Receive the APIVersions request
            data = client_socket.recv(1024)
            print(f"Received data: {data.hex()}")

            # Parse the correlation ID and API version from the request
            correlation_id = struct.unpack('>i', data[8:12])[0]
            api_key = struct.unpack('>h', data[8:10])[0]
            api_version = struct.unpack('>h', data[10:12])[0]
            print(f"Received correlation ID: {correlation_id}, API Key: {api_key}, API Version: {api_version}")

            # Prepare the response
            if api_version not in SUPPORTED_VERSIONS:
                # Error response for unsupported version
                error_message = b"Unsupported version"
                response = struct.pack('>iihh', 14 + len(error_message), correlation_id, UNSUPPORTED_VERSION_ERROR, len(error_message))
                response += error_message
            else:
                # Placeholder response for supported versions
                response = struct.pack('>iih', 6, correlation_id, 0)  # 0 is a placeholder for no error

            # Send the response
            client_socket.sendall(response)
            print(f"Sent response: {response.hex()}")

        except Exception as e:
            print(f"Error occurred: {e}")
        finally:
            client_socket.close()

if __name__ == "__main__":
    main()