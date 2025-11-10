#!/bin/bash
# Simple script to query PostgreSQL without interactive password prompt

export PGPASSWORD=finny_password
psql -h localhost -p 4320 -U finny_user -d finny_db "$@"