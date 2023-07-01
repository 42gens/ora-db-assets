from django.shortcuts import render
from django.views import View
from django.http import HttpResponse
import subprocess
#import logging

#logging.basicConfig(level=logging.DEBUG, filename="views.log") 

class OracleStateView(View):
    def get(self, request):
        return render(request, 'index.html')

    def post(self, request):
        host = request.POST.get('host')
        port = request.POST.get('port')
        service_name = request.POST.get('service_name')
        username = request.POST.get('username')
        password = request.POST.get('password')

        # Prepare the input string for the subprocess
        input_string = f"{host}\n{port}\n{service_name}\n{username}\n{password}\n"
        
        # Execute the script with the command-line arguments
        command = ['python', 'C:\\db-projects\\oracle_project\\ora_curr_state_v1.py', host, str(port), service_name, username, password]
        output = subprocess.check_output(command, input=input_string, text=True)
        
        # Store the output in session for downloading
        request.session['output'] = output
        
        # Call the script using subprocess
        return render(request, 'result.html', {'output': output})

def download_csv(request):
    output = request.session.get('output', '')
    response = HttpResponse(content_type='text/csv')
    response['Content-Disposition'] = 'attachment; filename="output.csv"'
    response.write(output)
    return response

def download_txt(request):
    output = request.session.get('output', '')
    response = HttpResponse(content_type='text/plain')
    response['Content-Disposition'] = 'attachment; filename="output.txt"'
    response.write(output)
    return response
