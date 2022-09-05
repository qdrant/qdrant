case $TARGETARCH in
  "amd64")
    echo "x86_64-unknown-linux-gnu"
    ;;
  "arm64")
    echo "aarch64-unknown-linux-gnu"
    ;;
esac

