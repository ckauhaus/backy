#!/bin/bash

set -e

HERE=$( cd $( dirname $0 ); pwd )
BACKY="${HERE}/bin/backy"
DIFF=/usr/bin/diff
BACKUP=$( mktemp -d -t backy.test )

mkdir ${BACKUP}/backup

echo "Using ${BACKUP} as workspace."

echo -n "Generating Test Data"
dd if=/dev/urandom of=$BACKUP/img_state1.img bs=1048576 count=20 2>/dev/null
echo -n "."
dd if=/dev/urandom of=$BACKUP/img_state2.img bs=1048576 count=20 2>/dev/null
echo -n "."
dd if=/dev/urandom of=$BACKUP/img_state3.img bs=1048576 count=20 2>/dev/null
echo " Done."


echo -n "Backing up img_state1.img. "
$BACKY backup $BACKUP/img_state1.img $BACKUP/backup
echo "Done."

echo -n "Restoring img_state1.img from level 0. "
$BACKY restore -r 0 $BACKUP/backup $BACKUP/restore_state1.img
echo "Done."

echo -n "Diffing restore_state1.img against img_state1.img. "
res=$($DIFF $BACKUP/restore_state1.img $BACKUP/img_state1.img)
if [ "" == "$res" ]; then
    echo "Success."
else
    echo "ERROR: $res"
    exit 2
fi

# cleanup
rm $BACKUP/restore_*




echo -n "Backing up img_state2.img. "
$BACKY backup $BACKUP/img_state2.img $BACKUP/backup
echo "Done."

echo -n "Restoring img_state2.img from level 0. "
$BACKY restore -r 0 $BACKUP/backup $BACKUP/restore_state2.img
echo "Done."

echo -n "Diffing restore_state2.img against img_state2.img. "
res=$($DIFF $BACKUP/restore_state2.img $BACKUP/img_state2.img)
if [ "" == "$res" ]; then
    echo "Success."
else
    echo "ERROR: $res"
    exit 2
fi

echo -n "Restoring img_state1.img from level 1. "
$BACKY restore -r 1 $BACKUP/backup $BACKUP/restore_state1.img
echo "Done."

echo -n "Diffing restore_state1.img against img_state1.img. "
res=$($DIFF $BACKUP/restore_state1.img $BACKUP/img_state1.img)
if [ "" == "$res" ]; then
    echo "Success."
else
    echo "ERROR: $res"
    exit 2
fi

# cleanup
rm $BACKUP/restore_*





echo -n "Backing up img_state2.img again. "
$BACKY backup $BACKUP/img_state2.img $BACKUP/backup
echo "Done."

echo -n "Restoring img_state2.img from level 0. "
$BACKY restore -r 0 $BACKUP/backup $BACKUP/restore_state2.img
echo "Done."

echo -n "Diffing restore_state2.img against img_state2.img. "
res=$($DIFF $BACKUP/restore_state2.img $BACKUP/img_state2.img)
if [ "" == "$res" ]; then
    echo "Success."
else
    echo "ERROR: $res"
    exit 2
fi

rm $BACKUP/restore_state2.img

echo -n "Restoring img_state2.img from level 1. "
$BACKY restore -r 1 $BACKUP/backup $BACKUP/restore_state2.img
echo "Done."

echo -n "Diffing restore_state2.img against img_state2.img. "
res=$($DIFF $BACKUP/restore_state2.img $BACKUP/img_state2.img)
if [ "" == "$res" ]; then
    echo "Success."
else
    echo "ERROR: $res"
    exit 2
fi

echo -n "Restoring img_state1.img from level 2. "
$BACKY restore -r 2 $BACKUP/backup  $BACKUP/restore_state1.img
echo "Done."

echo -n "Diffing restore_state1.img against img_state1.img. "
res=$($DIFF $BACKUP/restore_state1.img $BACKUP/img_state1.img)
if [ "" == "$res" ]; then
    echo "Success."
else
    echo "ERROR: $res"
    exit 2
fi

# cleanup
rm $BACKUP/restore_*



echo -n "Backing up img_state3.img. "
$BACKY backup $BACKUP/img_state3.img $BACKUP/backup
echo "Done."

echo -n "Restoring img_state3.img from level 0. "
$BACKY restore -r 0 $BACKUP/backup $BACKUP/restore_state3.img
echo "Done."

echo -n "Diffing restore_state3.img against img_state3.img. "
res=$($DIFF $BACKUP/restore_state3.img $BACKUP/img_state3.img)
if [ "" == "$res" ]; then
    echo "Success."
else
    echo "ERROR: $res"
    exit 2
fi

echo -n "Restoring img_state2.img from level 1. "
$BACKY restore -r 1 $BACKUP/backup $BACKUP/restore_state2.img
echo "Done."

echo -n "Diffing restore_state2.img against img_state2.img. "
res=$($DIFF $BACKUP/restore_state2.img $BACKUP/img_state2.img)
if [ "" == "$res" ]; then
    echo "Success."
else
    echo "ERROR: $res"
    exit 2
fi

rm $BACKUP/restore_state2.img

echo -n "Restoring img_state2.img from level 2. "
$BACKY restore -r 2 $BACKUP/backup $BACKUP/restore_state2.img
echo "Done."

echo -n "Diffing restore_state2.img against img_state2.img. "
res=$($DIFF $BACKUP/restore_state2.img $BACKUP/img_state2.img)
if [ "" == "$res" ]; then
    echo "Success."
else
    echo "ERROR: $res"
    exit 2
fi

echo -n "Restoring img_state1.img from level 3. "
$BACKY restore -r 3 $BACKUP/backup $BACKUP/restore_state1.img
echo "Done."

echo -n "Diffing restore_state1.img against img_state1.img. "
res=$($DIFF $BACKUP/restore_state1.img $BACKUP/img_state1.img)
if [ "" == "$res" ]; then
    echo "Success."
else
    echo "ERROR: $res"
    exit 2
fi

$BACKY scrub $BACKUP/backup all

$BACKY ls $BACKUP/backup

$BACKY clean $BACKUP/backup

$BACKY ls $BACKUP/backup

# cleanup
rm -r $BACKUP
