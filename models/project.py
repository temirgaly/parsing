from datetime import datetime


class Project:
    def __init__(self, row_data):
        self.id = row_data["id"]
        cell = row_data["cell"]

        self.project_id = cell[0] if len(cell) > 0 else None
        self.field_1 = cell[1] if len(cell) > 1 else None
        self.project_name = cell[2] if len(cell) > 2 else None
        self.customer = cell[3] if len(cell) > 3 else None
        self.contractor = cell[4] if len(cell) > 4 else None
        self.contract_number = cell[5] if len(cell) > 5 else None
        self.contract_date = self._parse_date(cell[6]) if len(cell) > 6 else None
        self.branch = cell[7] if len(cell) > 7 else None
        self.status = cell[8] if len(cell) > 8 else None
        self.field_9 = cell[9] if len(cell) > 9 else None
        self.note = cell[10] if len(cell) > 10 else None
        self.field_11 = cell[11] if len(cell) > 11 else None
        self.field_12 = cell[12] if len(cell) > 12 else None

    def _parse_date(self, date_str):
        """
        Convert date from DD.MM.YYYY to YYYY-MM-DD format.
        """
        if not date_str:
            return None
        try:
            dt = datetime.strptime(date_str, "%d.%m.%Y")
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            return None

    def __repr__(self):
        return f"Project(id={self.id}, name={self.project_name[:50]}...)"
