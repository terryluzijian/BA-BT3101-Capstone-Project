from wtforms import Form, StringField, SelectField, SelectMultipleField, IntegerField, PasswordField, validators

class BenchmarkerForm(Form):
    name = StringField('Professor Name', validators=[validators.input_required()])
    department = SelectField('Department', validators=[validators.input_required()],
        choices=[('Biomedical Engineering', 'Biomedical Engineering'), ('Biochemistry', 'Biochemistry'), ('Geography', 'Geography')])
    phd_year = IntegerField('PhD Year', validators=[validators.input_required()])
    phd_school = StringField('PhD University', validators=[validators.input_required()])
    text_raw = StringField('Research Areas', validators=[validators.input_required()])
    position = SelectField('Desired Promotion Level', validators=[validators.input_required()],
        choices=[('Associate Professor', 'Associate Professor'), ('Professor', 'Professor')])
    metrics = SelectMultipleField('Benchmarking Metrics', validators=[validators.input_required()], 
        choices=[('PHD YEAR', 'PhD Year'), ('PHD UNIVERSITY', 'Phd University'), 
            ('PROMO YEAR', 'Promotion Year'), ('RESEARCH AREA SIMILARITY', 'Research Area Similarity')])

class ChangePasswordForm(Form):
    old_password = PasswordField('Old Password', validators=[validators.input_required()])
    new_password = PasswordField('New Password', validators=[validators.input_required(), validators.equal_to('confirm', message='Passwords must match')])
    confirm = PasswordField('Repeat Password')