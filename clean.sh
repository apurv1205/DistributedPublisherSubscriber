#!/bin/bash
if [ -z "$(ls dataBackup)" ]; then
   :
else
   rm dataBackup/*
fi