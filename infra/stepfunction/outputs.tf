output "state_machine_arn" {
  description = "ARN da Step Function pipeline NYC TLC"
  value       = aws_sfn_state_machine.pipeline.arn
}

output "state_machine_name" {
  description = "Nome da Step Function pipeline NYC TLC"
  value       = aws_sfn_state_machine.pipeline.name
}
