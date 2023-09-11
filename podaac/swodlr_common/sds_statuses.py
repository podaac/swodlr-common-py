'''SDS job statuses grouped as sets'''
SUCCESS = {
    'job-completed'
}

FAIL = {
    'job-failed',
    'job-deduped',
    'job-timedout'  # Custom Swodlr status
}

WAITING = {
    'job-queued',
    'job-started',
    'job-offline',
}
