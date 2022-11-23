#!/bin/sh

# Save the current directory and the script's directory, since build must be run from the project root
pwd=$(pwd)
dirname=$(dirname $0)

# Back up the old PYTHONPATH so it can be restored later
old_pythonpath=$PYTHONPATH

# Build the program
cd $dirname/..
PYTHONPATH=
pip3 install -r requirements.txt
python3 setup.py bdist_mac

# Restore the old PYTHONPATH and move back to the original directory
cd $pwd
PYTHONPATH=$old_pythonpath

cd build
mv exe* mysqltoolsservice
cp mysqltoolsservice/lib* mysqltoolsservice/lib/.
tar -cvzf mysqltoolsservice-osx.tar.gz mysqltoolsservice
