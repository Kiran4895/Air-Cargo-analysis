# Air-Cargo-analysis

Objective:
The objective of this project is to analyse the Air Cargo database to identify regular customers, busiest routes, and ticket sales details. The project aims to provide insights that can be used to improve the airline's operability and customer service.

Data:
The database contains information on customers, passengers on flights, ticket details, and routes. The customer table contains customer details such as ID, first name, last name, date of birth, and gender. The passengers_on_flights table contains details of the passengers such as aircraft ID, route ID, customer ID, departure and arrival location, seat number, travel class, travel date, and flight number. The ticket_details table contains ticket purchase details such as purchase date, customer ID, aircraft ID, travel class, number of tickets, airport code, price per ticket, and brand. The routes table contains route details such as route ID, flight number, origin airport, destination airport, aircraft ID, and distance in miles.

Tasks:
1.	Create an ER diagram for the given airlines database.
2.	Write a query to create route_details table using suitable data types for the fields, such as route_id, flight_num, origin_airport, destination_airport, aircraft_id, and distance_miles. Implement the check constraint for the flight number and unique constraint for the route_id fields. Also, make sure that the distance miles field is greater than 0.
3.	Write a query to display all the passengers (customers) who have traveled in routes 01 to 25. Take data from the passengers_on_flights table.
4.	Write a query to identify the number of passengers and total revenue in business class from the ticket_details table.
5.	Write a query to display the full name of the customer by extracting the first name and last name from the customer table.
6.	Write a query to extract the customers who have registered and booked a ticket. Use data from the customer and ticket_details tables.
7.	Write a query to identify the customer’s first name and last name based on their customer ID and brand (Emirates) from the ticket_details table.
8.	Write a query to identify the customers who have travelled by Economy Plus class using Group By and Having clause on the passengers_on_flights table.
9.	Write a query to identify whether the revenue has crossed 10000 using the IF clause on the ticket_details table.
10.	Write a query to create and grant access to a new user to perform operations on a database.
11.	Write a query to find the maximum ticket price for each class using window functions on the ticket_details table.
12.	Write a query to extract the passengers whose route ID is 4 by improving the speed and performance of the passengers_on_flights table.
13.	For the route ID 4, write a query to view the execution plan of the passengers_on_flights table.
14.	Write a query to calculate the total price of all tickets booked by a customer across different aircraft IDs using rollup function.
15.	Write a query to create a view with only business class customers along with the brand of airlines.
16.	Write a query to create a stored procedure to get the details of all passengers flying between a range of routes defined in run time. Also, return an error message if the table doesn't exist.
17.	Write a query to create a stored procedure that extracts all the details from the routes table where the travelled distance is more than 2000 miles.
18.	Write a query to create a stored procedure that groups the distance travelled by each flight into three categories. The categories are, short distance travel (SDT) for >=0 AND <= 2000 miles, intermediate distance travel (IDT) for >2000 AND <=6500, and long-distance travel (LDT) for >6500.
19.	Write a query to extract ticket purchase date, customer ID, class ID and specify if the complimentary services are provided for the specific class using a stored function in stored procedure on the ticket_details table.
Condition:
If the class is Business and Economy Plus, then complimentary services are given as Yes, else it is No
20.	Write a query to extract the first record of the customer whose last name ends with Scott using a cursor from the customer table.
