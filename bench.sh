min() {
    printf "%s\n" "$@" | sort -g | head -n1
}

num_ops() {
  object_size=$1
  n_ops=$((2147483648 / object_size))
  min $n_ops 1000
}

for o in "134217728"; do
  n=`num_ops $o`
  python3 create_config.py $o $n
  echo "Object size: $o, #Ops: $n"
  ./read_write conf/crail-bench.ini
  echo "Finished: Object size: $o, #Ops: $n"
  sleep 3
done
