import pandas as pd
import pickle

class DataExtractor:
    def __init__(self, invoices_file, expired_invoices_file):
        self.invoices_file = invoices_file
        self.expired_invoices_file = expired_invoices_file
        self.invoices, self.expired_ids = self.load_data()
    
    def load_data(self):
        with open(self.invoices_file, 'rb') as f:
            invoices = pickle.load(f)
        with open(self.expired_invoices_file, 'r') as f:
            expired_invoice_ids = {int(line.strip()) for line in f if line.strip().isdigit()}
        return invoices, expired_invoice_ids
    
    def parse_quantity(self, quantity):
        try:
            return int(quantity)
        except ValueError:
            try:
                return self.words_to_int(quantity)
            except ValueError:
                return None
    
    def words_to_int(self, quantity):
        words = quantity.lower().strip().split()
        numbers = {
            "one": 1, "two": 2, "three": 3, "four": 4, "five": 5,
            "six": 6, "seven": 7, "eight": 8, "nine": 9, "ten": 10
        }
        return sum(numbers[word] for word in words)
    
    def transform_data(self):
        records = []
        type_conversion = {0: 'Material', 1: 'Equipment', 2: 'Service', 3: 'Other'}
        
        for invoice in self.invoices:
            invoice_id = invoice['id']
            if isinstance(invoice_id, str):
                invoice_id = invoice_id.rstrip('O')
            created_on = invoice['created_on']
            
            try:
                created_on = pd.to_datetime(created_on)
            except ValueError as e:
                print(f"Skipping invoice with invalid creation date: {e}: {created_on}")
                continue
            
            total_invoice_price = 0
            for item in invoice.get('items', []):
                try:
                    unit_price = item['item']['unit_price']
                    quantity = self.parse_quantity(item['quantity'])
                    if quantity is None:
                        print(f"Skipping item with invalid quantity: {item['quantity']}")
                        continue
                    total_invoice_price += unit_price * quantity
                except (KeyError, ValueError) as e:
                    print(f"Skipping item with issue: {e}: {item}")
                    continue
            
            is_expired = invoice_id in self.expired_ids
            
            for item in invoice.get('items', []):
                try:
                    invoiceitem_id = item['item']['id']
                    invoiceitem_name = item['item']['name']
                    type_ = type_conversion.get(item['item']['type'], 'Other')
                    unit_price = item['item']['unit_price']
                    quantity = self.parse_quantity(item['quantity'])
                    
                    if quantity is None:
                        print(f"Skipping item with invalid quantity: {item['quantity']}")
                        continue
                        
                    total_price = unit_price * quantity
                    percentage_in_invoice = total_price / total_invoice_price
                    
                    record = {
                        'invoice_id': int(invoice_id),
                        'created_on': created_on,
                        'invoiceitem_id': invoiceitem_id,
                        'invoiceitem_name': invoiceitem_name,
                        'type': type_,
                        'unit_price': unit_price,
                        'total_price': total_price,
                        'percentage_in_invoice': percentage_in_invoice,
                        'is_expired': is_expired
                    }
                    records.append(record)
                except (KeyError, ValueError) as e:
                    print(f"Skipping item with issue: {e}: {item}")
                    continue
        
        df = pd.DataFrame(records)
        df = df.astype({
            'invoice_id': 'int',
            'created_on': 'datetime64[ns]',
            'invoiceitem_id': 'int',
            'invoiceitem_name': 'str',
            'type': 'str',
            'unit_price': 'int',
            'total_price': 'int',
            'percentage_in_invoice': 'float',
            'is_expired': 'bool'
        })
        df = df.sort_values(by=['invoice_id', 'invoiceitem_id']).reset_index(drop=True)
        return df
    
    def save_to_csv(self, df, filename):
        df.to_csv(filename, index=False)
        print(f"DataFrame saved to {filename}")

def main():
    invoices_file = 'invoices_new.pkl'
    expired_invoices_file = 'expired_invoices.txt'

    extractor = DataExtractor(invoices_file, expired_invoices_file)
    df = extractor.transform_data()
    extractor.save_to_csv(df, 'transformed_invoices.csv')

if __name__ == "__main__":
    main()
