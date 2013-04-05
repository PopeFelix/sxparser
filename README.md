sxparser
========

SoundExchange Parser

This program will generate log files suitable for submission to SoundExchange via NPR Digital Services.  
While it is only designed to work with the setup at KTBG (http://www.ktbg.fm/), I hope that it might prove 
useful as a starting point for other log parsers.

The program will, for the most recently completed quarter, read playlist and streaming logs for a contiguous two 
week period sometime in the quarter.  To figure out what particular contigous two week period is used, consult the 
generated log files.  Future releases might well provide some way of reporting those dates. :)

The program assumes streaming is done via Windows Media Services, and automation via BSI Simian 1.6.  Additionally, 
it assumes a database accessed via ODBC.  
