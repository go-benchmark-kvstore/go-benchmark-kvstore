#!/bin/sh -e

cat .gitlab-ci-template-header.yml > generated-gitlab-ci.yml

for READERS in 1 50 100 ; do
  for WRITERS in 1 10 50 ; do
    for SIZE in 100B 32KB 5MB 500MB ; do
      for FS in ext4 xfs ; do
        for ENGINE in badger bbolt bitcask buntdb fs fsclone immudb nutsdb pebble sqlite ; do
          NAME="$ENGINE [$READERS $WRITERS $SIZE $FS]"
          sed \
            -e "s/__NAME__/${NAME}/g" \
            -e "s/__READERS__/${READERS}/g" \
            -e "s/__WRITERS__/${WRITERS}/g" \
            -e "s/__SIZE__/${SIZE}/g" \
            -e "s/__FS__/${FS}/g" \
            -e "s/__ENGINE__/${ENGINE}/g" \
            .gitlab-ci-template-entry.yml >> generated-gitlab-ci.yml
        done
        for ENGINE in postgres postgreslo ; do
          NAME="$ENGINE [$READERS $WRITERS $SIZE $FS]"
          sed \
            -e "s/__NAME__/${NAME}/g" \
            -e "s/__READERS__/${READERS}/g" \
            -e "s/__WRITERS__/${WRITERS}/g" \
            -e "s/__SIZE__/${SIZE}/g" \
            -e "s/__FS__/${FS}/g" \
            -e "s/__ENGINE__/${ENGINE}/g" \
            .gitlab-ci-template-entry-postgres.yml >> generated-gitlab-ci.yml
        done
      done
    done
  done
done
