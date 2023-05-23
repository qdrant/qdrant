case $TARGETARCH in
  "amd64")
    echo "./mold/bin/mold -run cargo"
    ;;
  "arm64")
    echo "cargo"
    ;;
esac
