#!/bin/bash
CODE_OPTION="--avx512code"
MODE_OPTION="--logfree --pipeline"
result_path="recovery.out"

num_obj_std=34774016 # 10G pobjpool for 256B

list_nodes=("0" "1")
list_objsize=("192" "448" "960" "1984" "4032")
list_num_thread=("1" "2" "4" "6" "8" "10" "12" "14" "16" "18")
list_parity=("1")


mv recovery.out recovery.out.old

# rebuild whole project
cd ../../..
echo "make clean -j &> /dev/null; ./runmake -mlpc $MODE_OPTION $CODE_OPTION &> /dev/null"
make clean -j &> /dev/null; ./runmake -mlpc $MODE_OPTION $CODE_OPTION &> /dev/null
cd examples/libpmemobj/simple_recovery
echo "make nondebug &> /dev/null"
make nondebug &> /dev/null

for node in "${list_nodes[@]}"; do
    for objsize in "${list_objsize[@]}"; do
        num_obj=$(echo "scale=0; $num_obj_std / (($objsize+64) / 256)" | bc)  # num_obj is depend on objsize!
        echo "make alloc NODE=$node OBJSIZE=$objsize NUMOBJ=$num_obj &> /dev/null"
        make alloc NODE=$node OBJSIZE=$objsize NUMOBJ=$num_obj &> /dev/null
        for parity in "${list_parity[@]}"; do
            for num_thread in "${list_num_thread[@]}"; do
                if [ $objsize -eq 4032 ] && [ $num_thread -ne 1 ]; then
                    make alloc NODE=$node OBJSIZE=$objsize NUMOBJ=$num_obj &> /dev/null
                fi
                echo "make recovery NODE=$node OBJSIZE=$objsize NUMOBJ=$num_obj NUMTHREAD=$num_thread NUMPARITY=$parity | tee -a $result_path"
                make recovery NODE=$node OBJSIZE=$objsize NUMOBJ=$num_obj NUMTHREAD=$num_thread NUMPARITY=$parity | tee -a $result_path
            done
        done
    done
done