Task 1.

Build 3 scripts that communicate with each other over zmq
Script A will be the strategy code that places orders
Script B will be the broker adapter
Script C will mock a broker 

The communication flow for placing an order must be from Script A to Script B, and then from Script B to Script C.
For receiving updates, the communication flow must be from script C to script B, and from script B to script A.
Please showcase a demo with 5 orders, including buy and sell. Script A should display realised PnL, the order book, and open positions


Task 2.



Build 2 scripts that communicate over rest protocol use fast api on Script A to host an API.
Script B will call the API.


Create one version using threading and another version using asyncio for Script A.