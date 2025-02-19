// src/main.rs
fn main() {
    // Get command line arguments (skip the first argument which is the program name)
    let args: Vec<String> = std::env::args().skip(1).collect();

    // Verify at least two numbers are provided
    if args.len() < 2 {
        eprintln!("Usage: cargo run -- <number1> <number2>");
        std::process::exit(1);
    }

    // Parse arguments to integers
    let num1: i32 = args[0].parse().expect("First argument must be an integer");
    let num2: i32 = args[1].parse().expect("Second argument must be an integer");

    // Calculate and print results
    let product = num1 * num2;
    let square_diff = num1.pow(2) - num2.pow(2);

    println!("===== RESULT =====");
    println!("Number 1: {}", num1);
    println!("Number 2: {}", num2);
    println!("Product: {}", product);
    println!("Squared Difference: {}", square_diff);
    println!("===================");
}