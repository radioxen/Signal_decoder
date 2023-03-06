# Software Challenge

> BETA Technologies Data Engineering Challenge

Please read the complete instructions before proceeding.

## Description


https://drive.google.com/file/d/1S8uiDGTaA6oTBs-Yy-Glz2ARsXYciW9v/view contains a gzipped csv file with data collected off of the CAN data buses of our Alia aircraft.
(if you do not have access, please contact josh@beta.team)

Each row contains a log of a single CAN message including a timestamp, the name of the bus the message was sent on, 
the ID of the message, its length, and the individual bytes of message payload (logged as hexadecimal values) 

### Part 1 -- Extr acting and Interpreting

For part 1 the task is to extract several signals of interest from the data provided here.

Specifically, we are interested in the pilot control inputs, which are sent in two messages:

- ID 0x7e3, which contains the following fields (in this order)
    - roll axis input as a 2 byte signed integer in `[-32768, 32767]` that corresponds to a commanded angle in `[-30, 30]` degrees
    - pitch axis input as a 2 byte signed integer in `[-32768, 32767]` that corresponds to a commanded angle in `[-30, 30]` degrees
    - yaw axis input as a 2 byte signed integer in `[-32768, 32767]` that corresponds to a commanded angular rate in `[-60, 60]` degrees/s
    - hover throttle as a 2 byte unsigned integer in `[0, 65535]` that corresponds to a throttle percentage

- ID 0x7e4, which contains the following fields (in this order)
    - propspin switch as a 1 byte unsigned integer in `[0, 255]` that corresponds to a bool
    - pusher throttle as a 2 byte signed integer in `[-32768, 32767]` that corresponds to +/- percentage, w/ 0 being neutral
    - some additional data that we can ignore

Additionally, we are interested in seeing data from our onboard inertial measurement unit (IMU):

- ID 0x1 contains the pitch angle (in radians) as a 4 byte float, followed by some data we can ignore
- ID 0x2 contains the pitch rate (in radians/s) as a 4 byte float, followed by some data we can ignore
- ID 0x3 contains the roll angle (in radians) as a 4 byte float, followed by some data we can ignore
- ID 0x4 contains the roll rate (in radians/s) as a 4 byte float, followed by some data we can ignore
- ID 0x5 contains the yaw angle (in radians) as a 4 byte float, followed by some data we can ignore
- ID 0x6 contains the yaw rate (in radians/s) as a 4 byte float, followed by some data we can ignore

**Note** all multi-byte values use a "big endian" byte order.

Your task here is to write code that extracts and interprets these signals and produces a new CSV file that 
- retains the timestamp and bus from the original file, 
- replaces the rest of the columns with a signal identifier and the interpreted value
- converts radians to degrees


This file should have columns: Timestamp,Bus,Signal,Value

Where signals are named as follows
roll_input, pitch_input, yaw_input, hover_throttle_input, propspin_input, pusher_throttle_input, pitch_angle, pitch_rate, roll_angle, roll_rate, yaw_angle, yaw_rate 

**Important**
- Your code must be capable of parallelizing the compute heavy portions of this workload across multiple cores of a single machine (or multiple machines) in order to process the data quickly
- The final result should be equivalent regardless of how many cores/machines it is parallelized across 
- Additionally, your code must be able to handle arbitrarily large amounts of data without running out of memory


It may be useful to save intermediate outputs of this part for use in Part 2.

### Part 2 -- Pivoting

The next step is to go from the "tall" format in use in Part 1 to a wide columnar format:
- There is a Timestamp column
- There is a Bus_Signal column for every (Bus, Signal) pair in the output of Part 1
- Every row contains bins of 10ms in duration with the most recent value for each (Bus, Signal) pair
  - timestamps should be even multiples of 10ms, where that row holds the most recent value received up to and including that time 
  - a column will have null values for all rows prior to when that signal is first received
- The final output should be an Apache Parquet file
    
**Once again**
- Your code must be capable of parallelizing the compute heavy portions of this workload across multiple cores of a single machine (or multiple machines) in order to process the data quickly
- The final result should be equivalent regardless of how many cores/machines it is parallelized across 
- Additionally, your code must be able to handle arbitrarily large amounts of data without running out of memory (likely trickier here than in Part 1)


Please submit to josh@beta.team your (appropriately documented and commented) code + a README containing 
  - complete instructions on how to run the code
  - any additional documentation that would be useful to understand your solution
  - a discussion of any problems encountered
  - (Possibly) figures/text addressing the bonus part (see below) 

**Note:** you may employ whatever open source tools that you need to complete this task.

### Bonus

As may or may not have been clear, each of these signals is redundant from different sensors communicating on different buses.
Considering these replicated signals across the different buses, what conclusions can you draw about the different replicates?
I.e. in what way are the signals different from each other and what is your interpretation of those differences?
