import pstats
p = pstats.Stats('startup.prof')
p.sort_stats('cumulative').print_stats(20) # Print top 20 time consumers