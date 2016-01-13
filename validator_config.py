def google_serp_validator(result):
	if result.status_code != 200 or result.json()['responseStatus'] != 200:
		return False
	return True

def default_validator(result):
	if result.status_code != 200:
		return False
	return True

validator = {'default':default_validator,'google_serp':google_serp_validator,'google_serp_seed':google_serp_validator,'test':google_serp_validator}
