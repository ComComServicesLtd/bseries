# ping, complete.
#
#   0-244    a reading, in milliseconds
#   245-247  a reading known only to a range, so an outlier costs one byte
#   248-253  ICMP outcomes: the probe was answered, but not with a reply
#   254      no reply at all
#   255      nothing was polled (the header owns this)
#
# Supersedes "ping", which named no ICMP outcome and left 248-253 meaning
# nothing -- so an unreachable host read as a 253ms measurement. Profiles are
# immutable, so this is a new one rather than an edit: a client that already
# fetched "ping" caches it forever.

literal  0-244  ms          #0000FF,#00FFFF,#00FF00,#FFFF00,#FF0000

bucket   245    245-500     "over 244 ms"    #FF8C00
bucket   246    500-1000    "over 500 ms"    #FF4500
bucket   247    1000-30000  "over 1000 ms"   #B22222

state    248    send_error    "Send error"                   #6A5ACD
state    249    other         "Other ICMP error"             #7B68EE
state    250    prohibited    "Administratively prohibited"  #8A2BE2
state    251    ttl_exceeded  "TTL exceeded"                 #9370DB
state    252    unreach_net   "Network unreachable"          #9932CC
state    253    unreach_host  "Host unreachable"             #BA55D3
state    254    no_reply      "No reply"                     #000000
