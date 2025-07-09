# Analyst report

**Project**: Customer Retention Program<br>
**Data Analyst**: Thorsten Weber<br>
**Date**: 09.07.2025

## 🗂️ Data model

### Cleansing approach

### Feature engineering

## 🧩 Retention Program  

### The concept

### Scoring model

### Segment analysis

## ⚠️ Data anomalies

During initial exploration of the data following anomalies have been detected. While we were able to clean and impute missing values we **highly recommend to take action to prevent future anomalies and improve data quality**.

* Negative or zero value for booked nights in hotel
  * Check_out_time within 24 hours of check_in_time causes the problems
* Check_out_time of hotels is often exactly 11:00 a.m.	hotels
  * Maybe a default setting or problem with data transfer
* 0 seats booked for airplane flights (~100 times)
  * Assumption: Might be caused by standby passengers
* Cancellation sessions always show both flight and hotel as canceled
  * Unsure if that is always the case, as users could cancel only the hotel (or flight) 