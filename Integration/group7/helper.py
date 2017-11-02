def get_peer_unis(dep):
    bme_peer = [
        'Columbia University',
        'Northwestern University',
        'Rice University',
        'University of California, Los Angeles (UCLA)',
        'University of Michigan',
        'University of Toronto']
        
    biochem_peer = [
        'University of Chicago',
        'The University of Hong Kong',
        'New York University',
        'Monash University'
    ]

    geo_peer = [
        'UCL (University College London)',
        'University of Colorado - Boulder',
        'The University of Manchester',
        'University of Toronto (St George)',
        'Queen Mary University of London']

    departments = {
        'bme': bme_peer,
        'biochem': biochem_peer,
        'geo': geo_peer}

    return departments[dep]

def get_asp_unis(dep):
    bme_asp = [
        'Johns Hopkins University',
        'Georgia Institute of Technology',
        'University of California, San Diego (UCSD)',
        'Duke University',
        'Massachusetts Institute of Technology',
        'Stanford University']
    
    biochem_asp = [
        'University of California, San Francisco',
        'University of College London',
        'University of Illinois at Urbana-Champaign',
        'McGill University'
    ]
     
    geo_asp = [
        'University of Cambridge',
        'University of British Columbia',
        'University of Oxford']

    departments = {
        'bme': bme_asp,
        'biochem': biochem_asp,
        'geo': geo_asp}

    return departments[dep]