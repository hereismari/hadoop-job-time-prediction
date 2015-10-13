#!/bin/bash

minNumArgs=3

if [ "$#" -lt "$minNumArgs" ]; then
  echo "Usage: $0 <InstanceIP> <PublicKeyPath> <PrivateKeyPairPath>" >&2
  exit 1
fi

instanceIP=$1
publicKeyPath=$2
keypairPath=$3

publicKeyTxt="\"""$(cat "$publicKeyPath")""\""
ssh -o StrictHostKeyChecking=no -i $keypairPath ubuntu@$instanceIP "bash -s" -- < ./configure_hadoop_user.sh $publicKeyTxt
