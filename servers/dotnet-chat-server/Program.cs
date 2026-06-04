using DotNetChatServer;

var builder = WebApplication.CreateBuilder(args);

// Add services to the container
builder.Services.AddSignalR();

// Configure CORS to allow any origin for testing
builder.Services.AddCors(options =>
{
    options.AddPolicy("AllowAll",
        policy =>
        {
            policy.AllowAnyOrigin()
                  .AllowAnyHeader()
                  .AllowAnyMethod();
        });
});

var app = builder.Build();

// Configure the HTTP request pipeline
app.UseCors("AllowAll");

app.MapHub<ChatHub>("/chathub");

app.MapGet("/", () => "SignalR Test Server is running. Connect to /chathub");

Console.WriteLine("SignalR Test Server starting...");
Console.WriteLine("Hub URL: http://localhost:5000/chathub");
Console.WriteLine("Press Ctrl+C to stop");
Console.WriteLine();

app.Run("http://localhost:5000");
