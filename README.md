unique-window-pair-mapping
==========================

Given a dmel genome that was broken up into windows of 20 nucleotides and 32 nucleotides we want to obtain all 5' positions where both window sequences map. 

We use ruby scripts execute via Sun Grid Engine jobs that coordinate via a dynamically launched redis instance to run this analysis in parallel. 

*This is a one-off script.*
