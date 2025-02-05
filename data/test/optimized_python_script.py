import time
from collections import defaultdict
from statistics import mean

def analyze_student_data_optimized():
    student_scores = [
        {"student_id": 1, "name": "John", "scores": [75, 82, 91, 65, 88]},
        {"student_id": 2, "name": "Emma", "scores": [95, 88, 92, 87, 90]},
        {"student_id": 3, "name": "Michael", "scores": [70, 65, 88, 92, 85]},
        {"student_id": 4, "name": "Sarah", "scores": [89, 91, 95, 88, 85]},
        {"student_id": 5, "name": "James", "scores": [78, 85, 91, 87, 83]},
    ] * 2000  

    start_time = time.time()

    student_averages = []
    top_students = []
    all_scores = []
    for student in student_scores:
        average = mean(student["scores"])
        student_averages.append({"id": student["student_id"], "name": student["name"], "average": average})
        all_scores.extend(student["scores"])
        if average > 85:
            top_students.append({"name": student["name"], "average": average})

    # Calculate class average using the total of all scores
    total_class_score = sum(all_scores)
    class_average = total_class_score / len(all_scores)

    # Efficient grade distribution using dictionary with default values
    grade_counts = defaultdict(int)
    for score in all_scores:
        if score >= 90:
            grade_counts["A"] += 1
        elif score >= 80:
            grade_counts["B"] += 1
        elif score >= 70:
            grade_counts["C"] += 1
        elif score >= 60:
            grade_counts["D"] += 1
        else:
            grade_counts["F"] += 1

    execution_time = time.time() - start_time
    
    return {
        "student_averages": student_averages,
        "top_students": top_students,
        "class_average": class_average,
        "grade_distribution": dict(grade_counts),
        "execution_time": execution_time
    }

# Run the optimized version
result = analyze_student_data_optimized()
print(f"Optimized Execution Time: {result['execution_time']:.4f} seconds")
