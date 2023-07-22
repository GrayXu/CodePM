KEY_TYPE=randint
mkdir -p workloads
# for WORKLOAD_TYPE in a u; do
# for WORKLOAD_TYPE in u; do
for WORKLOAD_TYPE in u u50 u30 u20 u5; do
  echo workload${WORKLOAD_TYPE} > workload_config.inp
  echo ${KEY_TYPE} >> workload_config.inp
  python2 gen_workload.py workload_config.inp
  mv workloads/load_${KEY_TYPE}_workload${WORKLOAD_TYPE} workloads/load${WORKLOAD_TYPE}_unif_int.dat
  mv workloads/txn_${KEY_TYPE}_workload${WORKLOAD_TYPE} workloads/txns${WORKLOAD_TYPE}_unif_int.dat
done