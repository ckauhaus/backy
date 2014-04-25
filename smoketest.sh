#!/bin/bash

set -e

HERE=$( cd $( dirname $0 ); pwd )
BACKY="${HERE}/bin/backy"
DIFF=/usr/bin/diff
BACKUP=$( mktemp -d -t backy.test.XXXXX )

mkdir ${BACKUP}/backup
cd ${BACKUP}/backup

# $BACKY init ../original
cat > config <<EOF
{"source": "../original", "chunksize": 4194304, "source-type": "plain", "interval": 0}
EOF

echo "Using ${BACKUP} as workspace."

echo -n "Generating Test Data"
dd if=/dev/urandom of=$BACKUP/img_state1.img bs=1048576 count=20 2>/dev/null
echo -n "."
dd if=/dev/urandom of=$BACKUP/img_state2.img bs=1048576 count=20 2>/dev/null
echo -n "."
dd if=/dev/urandom of=$BACKUP/img_state3.img bs=1048576 count=20 2>/dev/null
echo " Done."

echo -n "Backing up img_state1.img. "
ln -sf img_state1.img ../original
$BACKY backup
echo "Done."

echo -n "Restoring img_state1.img from level 0. "
$BACKY restore -r 0 ../restore_state1.img
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
ln -sf img_state2.img ../original
$BACKY backup
echo "Done."

echo -n "Restoring img_state2.img from level 0. "
$BACKY restore -r 0 ../restore_state2.img
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
$BACKY restore -r 1 ../restore_state1.img
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
ln -sf img_state2.img ../original
$BACKY backup
echo "Done."

echo -n "Restoring img_state2.img from level 0. "
$BACKY restore -r 0 ../restore_state2.img
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
$BACKY restore -r 1 ../restore_state2.img
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
$BACKY restore -r 2 ../restore_state1.img
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
ln -sf img_state3.img ../original
$BACKY backup
echo "Done."

echo -n "Restoring img_state3.img from level 0. "
$BACKY restore -r 0 ../restore_state3.img
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
$BACKY restore -r 1 ../restore_state2.img
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
$BACKY restore -r 2 ../restore_state2.img
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
$BACKY restore -r 3 ../restore_state1.img
echo "Done."

echo -n "Diffing restore_state1.img against img_state1.img. "
res=$($DIFF $BACKUP/restore_state1.img $BACKUP/img_state1.img)
if [ "" == "$res" ]; then
    echo "Success."
else
    echo "ERROR: $res"
    exit 2
fi

$BACKY scrub all

$BACKY status

$BACKY maintenance

$BACKY status

# cleanup
if [ "$1" != "keep" ]; then
    rm -rf $BACKUP
fi
