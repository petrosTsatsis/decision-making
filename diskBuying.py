import numpy as np
import pandas as pd
import random


user_data = pd.read_csv('/home/petros/Downloads/user_money_rates.csv', header=None)
disc_cost = pd.read_csv('/home/petros/Downloads/album_price.csv', header=None)

# Define the number of users and discs
num_users = user_data.shape[0]
num_discs = disc_cost.shape[1]

# Extract the budget data from the user data
budget_data = user_data.iloc[:, 0]
# Define the budget limit for each user
budget_limit = budget_data.values

# Generate population based on rates and budget
population_size = num_users
population = []
for _ in range(population_size):
    individual = []

    for disc_rate, disc_price in zip(user_data.iloc[:, 1:].values.tolist()[0], disc_cost.iloc[:, 0].values.tolist()):
        if disc_price <= budget_data.iloc[_]:  # Check if disc price is within the user's budget
            individual.append(1)  # Select the disc
        else:
            individual.append(0)  # Do not select the disc
    population.append(individual)

num_iterations = 100

def evaluate_individual(individual, user_data, disc_cost, budget_limit):
    disc_prices = np.array(disc_cost.iloc[:, 0])
    individual = np.array(individual)
    total_cost = sum(individual[i] * disc_prices[i] for i in range(len(individual)))

    if total_cost > budget_limit:
        # If the total cost exceeds the budget, penalize the individual with a negative fitness value
        return [-1] * len(user_data)

    user_ratings = user_data.iloc[:, 1:].values
    fitness_values = np.sum(individual * user_ratings, axis=1)
    fitness_values = np.maximum(fitness_values, 0)

    return fitness_values.tolist()

def crossover(parents, num_offspring):
    offspring = []
    num_genes = len(parents[0])
    for _ in range(num_offspring):
        parent1, parent2 = random.choices(parents, k=2)
        split_point = random.randint(1, num_genes -1 )
        child = parent1[:split_point] + parent2[split_point:]
        offspring.append(child)
    return offspring

def mutate(individual, mutation_rate):
    mutated_individual = individual.copy()
    for i in range(len(mutated_individual)):
        if random.random() < mutation_rate:
            mutated_individual[i] = 1 - mutated_individual[i]
    return mutated_individual

# Define the fitness function to maximize user's desire
def fitness_function(individual, user_data, disc_cost, budget_limit):
    fitness_values = evaluate_individual(individual, user_data, disc_cost, budget_limit)

    if any(value < 0 for value in fitness_values):
         #If any fitness value is negative, return a very low fitness value
        return -1

    return sum(fitness_values)

def random_selection(num_discs, num_selected):
    selected_discs = random.sample(range(1, num_discs + 1), num_selected)
    return selected_discs

for iteration in range(num_iterations):

    if iteration < len(budget_limit):
        current_budget_limit = budget_limit[iteration]
    else:
        current_budget_limit = budget_limit[-1]  # Use the last value when out of bounds

    fitness_values = [fitness_function(individual, user_data, disc_cost, current_budget_limit) for individual in population]

    if any(value < 0 for value in fitness_values):
        # At least one fitness value is negative assign a small positive fitness value to all individuals
        fitness_values = [0.01] * len(population)
    if len(fitness_values) < len(population):
        # Extend fitness_values to match the length of population
        fitness_values.extend([0.01] * (len(population) - len(fitness_values)))
    elif len(fitness_values) > len(population):
        # Truncate fitness_values to match the length of population
        fitness_values = fitness_values[:len(population)]

    # Apply the budget limit
    for i in range(len(population)):
        individual = population[i]
        disc_prices = np.array(disc_cost.iloc[:, 0])
        total_cost = sum(individual[j] * disc_prices[j] for j in range(len(individual)))
        if total_cost > current_budget_limit:
            # If the total cost exceeds the budget, randomly remove discs until the cost is within the budget
            indices = [j for j in range(len(individual)) if individual[j] == 1]  # Get the indices of selected discs
            random.shuffle(indices)
            for j in indices:
                individual[j] = 0
                total_cost -= disc_prices[j]
                if total_cost <= current_budget_limit:
                    break

    parents = random.choices(population, weights=fitness_values, k=population_size // 2)

    # Crossover
    offspring = crossover(parents, population_size - len(parents))

    # Mutation
    mutation_rate = 0.01
    offspring = [mutate(individual, mutation_rate) for individual in offspring]

    # Update population
    population = parents + offspring

    best_individual = population[fitness_values.index(max(fitness_values))]
    selected_discs_indices = [i for i, val in enumerate(best_individual) if val == 1]  # Get the indices of selected discs
    selected_discs = [i + 1 for i in selected_discs_indices]  # Get the disc numbers of selected discs
    total_fitness = sum([user_data.iloc[:, 1][i] for i in selected_discs_indices])
    print(f"User {iteration + 1}: Best Individual - {selected_discs}, Fitness - {total_fitness}")

    # Random Selection
    selected_discs_indices = [j for j, val in enumerate(individual) if val == 1]  # Get the indices of selected discs

    # Filter disc indices based on budget limit
    budget_filtered_discs_indices = [j for j in selected_discs_indices if disc_cost.iloc[j, 0] <= current_budget_limit]

    # Randomly select discs within the budget limit
    random_selected_discs = random.sample(budget_filtered_discs_indices, sum(individual))  # Select the same number of discs as the individual
    random_selected_discs = [j + 1 for j in random_selected_discs]  # Get the disc numbers of selected discs
    random_total_fitness = sum([user_data.iloc[:, 1][j] for j in random_selected_discs])  # Calculate the total fitness based on preferences in random selection
    print(f"User {iteration + 1} Random Selection: Selected Discs - {random_selected_discs}, Fitness - {random_total_fitness}\n")



