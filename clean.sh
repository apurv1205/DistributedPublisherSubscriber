#!/bin/bash
if [ -z "$(ls -A dataBackup)" ]; then
   :
else
   rm dataBackup/*
fi