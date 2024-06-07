#!/bin/bash

run_protoc() {
    go mod download

    protoc -I service/ -I pbtypes/ service/*.proto --go-grpc_out=./ --go_out=./


    mkdir build_proto
    protoc -I pbtypes/ pbtypes/*.proto --go_out=./build_proto/
    cp build_proto/gitlab.switch.ch/ub-unibas/go-ublicense/pbtypes/*.pb.go pbtypes/
    rm -rf build_proto
}

watch_proto() {
    echo "watch : not yet implemented, running as standard"
    run_protoc
}

usage() {
    echo "usage : build.sh [options]"
    echo "      --watch, -w : watch file changes to recompile (dev mode)"
}

watch=0

while [[ -n "$1" ]]; do
    case $1 in
        --watch)    watch=1
                    ;;
        -w) watch=1
            ;;
        *)  usage
            exit 1
            ;;
    esac
    shift
done

if [[ $watch -eq 1 ]]
then watch_proto ./proto/
else run_protoc
fi
