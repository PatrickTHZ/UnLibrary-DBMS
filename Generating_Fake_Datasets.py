import pandas as pd
from faker import Faker
import numpy as np

fake = Faker()

# User data with Sydney-specific details
user_data_adjusted = {
    "UserID": range(1, 101),
    "F_Name": [fake.first_name() for _ in range(100)],
    "L_Name": [fake.last_name() for _ in range(100)],
    "Street": [fake.street_name() + " Street" for _ in range(100)],
    "Suburb": [fake.city_suffix() + " Sydney" for _ in range(100)],
    "Postcode": [fake.random_int(min=2000, max=2234) for _ in range(100)],
    "State": ['NSW' for _ in range(100)],
    "Phone": ['02' + str(fake.random_number(digits=8)) for _ in range(100)],
    "Gender": [fake.random_element(elements=('M', 'F')) for _ in range(100)],
    "DOB": [fake.date_of_birth(minimum_age=18, maximum_age=65).strftime('%Y-%m-%d') for _ in range(100)]
}
user_df_adjusted = pd.DataFrame(user_data_adjusted)

# Staff data with librarian positions
staff_positions = ['Librarian', 'Assistant Librarian', 'Senior Librarian', 'Cataloging Librarian']
staff_data_adjusted = {
    "UserID": range(1, 101),
    "Position": [fake.random_element(elements=staff_positions) for _ in range(100)]
}
staff_df_adjusted = pd.DataFrame(staff_data_adjusted)

laptop_data = {
    "LaptopID": range(10000000000000, 10000000000100),
    "Name": [fake.company() + " Laptop" for _ in range(100)],
    "Description": [fake.text(max_nb_chars=200) for _ in range(100)]
}
laptop_df = pd.DataFrame(laptop_data)

# LaptopBorrowingTime data
laptop_borrowing_time_data = {
    "LaptopID": np.random.choice(laptop_data["LaptopID"], size=100, replace=True),
    "StartDate": [fake.date_time_between(start_date='-1y', end_date='now').strftime('%Y-%m-%d %H:%M:%S') for _ in range(100)],
    "EndDate": [fake.date_time_between(start_date='now', end_date='+1y').strftime('%Y-%m-%d %H:%M:%S') for _ in range(100)]
}
laptop_borrowing_time_df = pd.DataFrame(laptop_borrowing_time_data)

# Additional data tables
publisher_data = {
    "PublisherID": range(1, 101),
    "Name": [fake.company() for _ in range(100)],
    "PublicationYear": [fake.year() for _ in range(100)]
}
publisher_df = pd.DataFrame(publisher_data)

language_data = {
    "LanguageID": range(1, 101),
    "Name": [fake.language_name() for _ in range(100)]
}
language_df = pd.DataFrame(language_data)

author_data = {
    "AuthorID": range(1, 101),
    "Author_F_Name": [fake.first_name() for _ in range(100)],
    "Author_L_Name": [fake.last_name() for _ in range(100)]
}
author_df = pd.DataFrame(author_data)

section_data = {
    "SectionID": range(1, 101),
    "Name": [fake.bs() for _ in range(100)]
}
section_df = pd.DataFrame(section_data)

category_data = {
    "CategoryID": range(1, 101),
    "Category": [fake.word() for _ in range(100)]
}
category_df = pd.DataFrame(category_data)

books_data = {
    "ISBN": [fake.isbn13() for _ in range(100)],
    "Title": [fake.sentence(nb_words=4) for _ in range(100)],
    "Edition": [fake.random_int(min=1, max=10) for _ in range(100)],
    "AuthorID": np.random.randint(1, 101, size=100),
    "PublisherID": np.random.randint(1, 101, size=100),
    "LanguageID": np.random.randint(1, 101, size=100),
    "CategoryID": np.random.randint(1, 101, size=100),
    "SectionID": np.random.randint(1, 101, size=100)
}
books_df = pd.DataFrame(books_data)

ebook_data = {
    "EBookID": range(1, 101),
    "ISBN": [fake.isbn13() for _ in range(100)],
    "Extension": [fake.random_element(elements=['pdf', 'epub', 'mobi']) for _ in range(100)]
}
ebook_df = pd.DataFrame(ebook_data)

# Save all dataframes to CSV
user_df_adjusted.to_csv('user_data.csv', index=False)
staff_df_adjusted.to_csv('staff_data.csv', index=False)
laptop_borrowing_time_df.to_csv('laptop_borrowing_time_data.csv', index=False)
publisher_df.to_csv('publisher_data.csv', index=False)
language_df.to_csv('language_data.csv', index=False)
author_df.to_csv('author_data.csv', index=False)
section_df.to_csv('section_data.csv', index=False)
category_df.to_csv('category_data.csv', index=False)
books_df.to_csv('books_data.csv', index=False)
ebook_df.to_csv('ebook_data.csv', index=False)
laptop_df.to_csv('laptop_data.csv', index=False)