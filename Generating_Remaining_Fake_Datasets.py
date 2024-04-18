import pandas as pd
from faker import Faker
import numpy as np

fake = Faker()

# Data for Student table
student_data = {
    "UserID": range(1, 101),
    "FacultyID": np.random.randint(1, 101, size=100)
}
student_df = pd.DataFrame(student_data)

# Data for Faculties table
faculties_data = {
    "FacultyID": range(1, 101),
    "FacultyName": [fake.company() for _ in range(100)],
    "Department": [fake.job() for _ in range(100)]
}
faculties_df = pd.DataFrame(faculties_data)

# Data for Authentication table
authentication_data = {
    "UserID": range(1, 101),
    "Password": [fake.password() for _ in range(100)]
}
authentication_df = pd.DataFrame(authentication_data)

# Data for BookBorrowingTime table
book_borrowing_time_data = {
    "ISBN": [fake.isbn13() for _ in range(100)],
    "UserID": np.random.randint(1, 101, size=100),
    "StartDate": [fake.date_time_this_decade(before_now=True, after_now=False).isoformat() for _ in range(100)],
    "EndDate": [fake.date_time_this_decade(before_now=False, after_now=True).isoformat() for _ in range(100)]
}
book_borrowing_time_df = pd.DataFrame(book_borrowing_time_data)

# Data for Fine table
fine_data = {
    "FineNo": range(1, 101),
    "TransactionID": np.random.randint(1, 101, size=100),
    "Amount": [round(fake.pydecimal(left_digits=2, right_digits=2, positive=True), 2) for _ in range(100)],
    "Date": [fake.date_this_year().isoformat() for _ in range(100)]
}
fine_df = pd.DataFrame(fine_data)

# Data for TransactionChart table
transaction_chart_data = {
    "TransactionID": range(1, 101),
    "UserID": np.random.randint(1, 101, size=100),
    "ISBN": [fake.isbn13() for _ in range(100)],
    "LaptopID": np.random.randint(10000000000000, 10000000000100, size=100)
}
transaction_chart_df = pd.DataFrame(transaction_chart_data)

# Saving data to CSV
student_df.to_csv('student_data.csv', index=False)
faculties_df.to_csv('faculties_data.csv', index=False)
authentication_df.to_csv('authentication_data.csv', index=False)
book_borrowing_time_df.to_csv('book_borrowing_time_data.csv', index=False)
fine_df.to_csv('fine_data.csv', index=False)
transaction_chart_df.to_csv('transaction_chart_data.csv', index=False)
