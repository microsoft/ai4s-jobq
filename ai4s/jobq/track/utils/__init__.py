# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
#!/usr/bin/env python
# -*- coding: utf-8 -*-


def adaptive_interval(timedelta):
    if timedelta.total_seconds() < 8 * 3600:
        return "15m"
    if timedelta.total_seconds() < 24 * 3600:
        return "30m"
    if timedelta.total_seconds() < 2 * 24 * 3600:
        return "60m"
    if timedelta.total_seconds() < 4 * 24 * 3600:
        return "2h"
    if timedelta.total_seconds() < 7 * 24 * 3600:
        return "4h"
    if timedelta.total_seconds() < 14 * 24 * 3600:
        return "8h"
    return "12h"
