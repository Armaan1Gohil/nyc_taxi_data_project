FROM trinodb/trino:449

# Copy configuration files
COPY etc /etc/trino

# Set the entrypoint to Trino
ENTRYPOINT ["/usr/lib/trino/bin/launcher", "run", "--etc-dir", "/etc/trino"]