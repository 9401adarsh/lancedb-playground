# To Do List

- [x] Ensure you run each test mode for N times rather than just once. Write the logs to a file for better comprehension. Accept N as a user parameter. 
- [ ] More knobs for testing ? To Discuss with Yuval - add index type, and see if we can benchmark across different index types.
- [ ] Currently we make the index using Index::Auto, need to ensure we can benchmark across different Index Types. Need to look into this more.
- [ ] Python Scripts for plotting logs. 
- [x] Add a mode without no session. 
- [ ] Remove per table mode -> as it builds a session object atop of a separate connection object.