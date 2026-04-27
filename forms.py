from flask_wtf import FlaskForm
from wtforms import StringField, SubmitField
from wtforms.validators import DataRequired


class SubmitForm(FlaskForm):
    url = StringField("url", validators=[DataRequired()])
    submit = SubmitField("Submit")


class UpdateForm(FlaskForm):
    submit = SubmitField("Update Prices")
