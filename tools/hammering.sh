rm -f out.txt
seq 1 32 |
	xargs -n1 -P8 -I{} \
	sh -c '
		cargo test -p collection --lib collection_manager::collection_updater;
		printf "Run %02d/32: $?\n" "$0" | tee -a out.txt;
	' "{}"
echo
echo
sort out.txt
rm -f out.txt
